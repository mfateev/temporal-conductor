package io.temporal.conductor.poc3.workflow

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.netflix.conductor.common.metadata.tasks.TaskDef
import com.netflix.conductor.common.metadata.tasks.TaskType
import com.netflix.conductor.common.metadata.workflow.WorkflowDef
import com.netflix.conductor.common.metadata.workflow.WorkflowTask
import com.netflix.conductor.core.execution.DeciderOutcomeAccessor
import com.netflix.conductor.core.execution.DeciderService
import com.netflix.conductor.model.TaskModel
import com.netflix.conductor.model.WorkflowModel
import io.temporal.activity.ActivityOptions
import io.temporal.common.RetryOptions
import io.temporal.conductor.ext.InMemoryMetadataDAO
import io.temporal.conductor.ext.SystemTaskExecutor
import io.temporal.conductor.ext.TemporalDeciderServiceFactory
import io.temporal.conductor.poc3.activities.TaskExecutionActivities
import io.temporal.conductor.poc3.model.ConductorWorkflowInput
import io.temporal.conductor.poc3.model.ConductorWorkflowOutput
import io.temporal.workflow.Async
import io.temporal.workflow.Promise
import io.temporal.workflow.Workflow
import org.slf4j.LoggerFactory
import java.time.Duration

/**
 * Implementation of ConductorWorkflow that runs Conductor's DeciderService
 * within Temporal's workflow execution model.
 *
 * Architecture:
 * - DeciderService: Handles workflow scheduling decisions (what tasks to run next)
 * - SystemTaskExecutor: Delegates to Conductor's native system task implementations
 * - Temporal Activities: Execute worker tasks via Temporal's durable execution
 *
 * This implementation focuses on orchestration, delegating task execution logic
 * to Conductor's components.
 */
class ConductorWorkflowImpl : ConductorWorkflow {

    private val logger = LoggerFactory.getLogger(ConductorWorkflowImpl::class.java)
    private val objectMapper = jacksonObjectMapper()

    // Conductor components
    private lateinit var workflowModel: WorkflowModel
    private lateinit var deciderService: DeciderService
    private lateinit var metadataDAO: InMemoryMetadataDAO
    private lateinit var systemTaskExecutor: SystemTaskExecutor

    // Workflow state
    private val taskOutputs = mutableMapOf<String, Map<String, Any>>()
    private val pendingSignals = mutableMapOf<String, Map<String, Any>>()

    // Activity stub for worker task execution
    private val activities: TaskExecutionActivities = Workflow.newActivityStub(
        TaskExecutionActivities::class.java,
        ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofMinutes(10))
            .setRetryOptions(
                RetryOptions.newBuilder()
                    .setMaximumAttempts(3)
                    .build()
            )
            .build()
    )

    override fun execute(input: ConductorWorkflowInput): ConductorWorkflowOutput {
        val workflowInfo = Workflow.getInfo()
        val workflowRunId = workflowInfo.runId

        logger.info("Starting Conductor workflow execution, runId: {}", workflowRunId)

        // Initialize Conductor components
        initializeConductorComponents(input, workflowRunId, workflowInfo.workflowId)

        // Main scheduling loop
        try {
            runSchedulingLoop()
        } catch (e: com.netflix.conductor.core.exception.TerminateWorkflowException) {
            handleTerminateException(e)
        } catch (e: Exception) {
            logger.error("Workflow execution failed", e)
            workflowModel.status = WorkflowModel.Status.FAILED
            workflowModel.reasonForIncompletion = e.message
        }

        return ConductorWorkflowOutput(
            status = workflowModel.status.name,
            output = workflowModel.output ?: emptyMap(),
            failureReason = workflowModel.reasonForIncompletion,
            taskOutputs = taskOutputs.toMap()
        )
    }

    private fun initializeConductorComponents(
        input: ConductorWorkflowInput,
        workflowRunId: String,
        workflowId: String
    ) {
        // Initialize metadata DAO with task definitions
        metadataDAO = InMemoryMetadataDAO()
        input.taskDefsJson.forEach { (_, json) ->
            val taskDef = objectMapper.readValue<TaskDef>(json)
            metadataDAO.createTaskDef(taskDef)
        }

        // Create DeciderService with deterministic ID generation
        val factory = TemporalDeciderServiceFactory.forTemporalWorkflow(
            metadataDAO = metadataDAO,
            workflowRunId = workflowRunId
        )
        deciderService = factory.create()

        // Create SystemTaskExecutor with DeciderService for system task execution
        // This enables using Conductor's native implementations for all system tasks
        systemTaskExecutor = SystemTaskExecutor(deciderService, objectMapper)

        // Parse and preprocess workflow definition
        val workflowDef = objectMapper.readValue<WorkflowDef>(input.workflowDefJson)
        populateTaskDefinitions(workflowDef, input.taskDefsJson)
        disableConductorTimeouts(workflowDef)

        // Initialize workflow model
        workflowModel = WorkflowModel().apply {
            this.workflowId = workflowId
            workflowDefinition = workflowDef
            status = WorkflowModel.Status.RUNNING
            this.input = input.workflowInput
            createTime = Workflow.currentTimeMillis()
        }
    }

    private fun handleTerminateException(e: com.netflix.conductor.core.exception.TerminateWorkflowException) {
        if (e.message?.contains("No tasks found") == true) {
            logger.info("Empty workflow completed (no tasks to execute)")
            workflowModel.status = WorkflowModel.Status.COMPLETED
        } else {
            logger.error("Workflow terminated: {}", e.message)
            workflowModel.status = WorkflowModel.Status.TERMINATED
            workflowModel.reasonForIncompletion = e.message
        }
    }

    private fun runSchedulingLoop() {
        var iterations = 0
        val maxIterations = 1000

        while (!workflowModel.status.isTerminal && iterations < maxIterations) {
            iterations++
            logger.debug("Scheduling loop iteration {}", iterations)

            // Get scheduling decision from DeciderService
            val outcome = deciderService.decide(workflowModel)

            if (DeciderOutcomeAccessor.isComplete(outcome)) {
                completeWorkflow()
                break
            }

            // Apply task updates from DeciderService
            val tasksToUpdate = DeciderOutcomeAccessor.getTasksToBeUpdated(outcome)
            tasksToUpdate.forEach { updateTaskInWorkflow(it) }

            val tasksToSchedule = DeciderOutcomeAccessor.getTasksToBeScheduled(outcome)

            if (tasksToSchedule.isEmpty()) {
                if (shouldCompleteWorkflow()) {
                    completeWorkflow()
                    break
                }
                waitForSignalsOrChanges()
                continue
            }

            // Add tasks to workflow model
            tasksToSchedule.forEach { task ->
                task.workflowInstanceId = workflowModel.workflowId
                workflowModel.tasks.add(task)
            }

            // Execute tasks
            executeTasks(tasksToSchedule)

            // Re-evaluate IN_PROGRESS system tasks (JOIN, DO_WHILE, etc.)
            // This may schedule new tasks (e.g., DO_WHILE scheduling loop body tasks)
            reEvaluateInProgressSystemTasks()

            // Execute any tasks scheduled by system task re-evaluation
            executeNewlyScheduledTasks()
        }

        if (iterations >= maxIterations) {
            logger.error("Scheduling loop exceeded max iterations")
            workflowModel.status = WorkflowModel.Status.FAILED
            workflowModel.reasonForIncompletion = "Scheduling loop exceeded max iterations"
        }
    }

    private fun shouldCompleteWorkflow(): Boolean {
        val allTasksCompleted = workflowModel.tasks.all {
            it.status == TaskModel.Status.COMPLETED || it.status == TaskModel.Status.SKIPPED
        }
        val hasAsyncSystemTasksInProgress = workflowModel.tasks.any {
            systemTaskExecutor.isAsync(it.taskType) && it.status == TaskModel.Status.IN_PROGRESS
        }
        return allTasksCompleted && !hasAsyncSystemTasksInProgress
    }

    private fun completeWorkflow() {
        logger.info("Workflow completed")
        workflowModel.status = WorkflowModel.Status.COMPLETED
        val lastTask = workflowModel.tasks.lastOrNull { it.status == TaskModel.Status.COMPLETED }
        if (lastTask != null && workflowModel.output.isNullOrEmpty()) {
            workflowModel.output = lastTask.outputData
        }
    }

    private fun waitForSignalsOrChanges() {
        logger.debug("No tasks to schedule, waiting...")
        val hasSignal = Workflow.await(Duration.ofSeconds(1)) {
            pendingSignals.isNotEmpty()
        }
        if (hasSignal) {
            processSignals()
        }
    }

    private fun executeTasks(tasks: List<TaskModel>) {
        // Track task IDs we're about to process to detect newly added tasks
        val taskIdsToProcess = tasks.map { it.taskId }.toSet()

        // Separate system tasks from worker tasks
        val (systemTasks, workerTasks) = tasks.partition { isSystemTask(it) }

        // Separate async system tasks (like JOIN) that need to wait for worker tasks
        val (asyncSystemTasks, syncSystemTasks) = systemTasks.partition {
            systemTaskExecutor.isAsync(it.taskType)
        }

        // Execute sync system tasks first (FORK, SWITCH, etc.)
        // This includes DO_WHILE which may add new tasks via scheduleNextIteration()
        syncSystemTasks.forEach { executeSystemTask(it) }

        // Check for tasks added by system tasks (e.g., DO_WHILE loop body tasks)
        // These tasks are added directly to workflow.tasks by scheduleNextIteration()
        val newlyScheduledTasks = workflowModel.tasks.filter {
            it.status == TaskModel.Status.SCHEDULED && !taskIdsToProcess.contains(it.taskId)
        }

        // Combine original worker tasks with newly scheduled tasks
        val allWorkerTasks = workerTasks + newlyScheduledTasks.filter { !isSystemTask(it) }
        val newSystemTasks = newlyScheduledTasks.filter { isSystemTask(it) && !systemTaskExecutor.isAsync(it.taskType) }

        // Execute any new sync system tasks that were added
        newSystemTasks.forEach { executeSystemTask(it) }

        // Execute worker tasks (original + newly added)
        executeWorkerTasks(allWorkerTasks)

        // Execute async system tasks after worker tasks (JOIN)
        asyncSystemTasks.forEach { executeSystemTask(it) }
    }

    private fun isSystemTask(task: TaskModel): Boolean {
        return SystemTaskExecutor.isKnownSystemTaskType(task.taskType)
    }

    /**
     * Execute a system task using Conductor's native implementation.
     * All system tasks use Conductor's implementations with in-memory adapters.
     */
    private fun executeSystemTask(task: TaskModel) {
        logger.debug("Executing system task: {} ({})", task.referenceTaskName, task.taskType)

        // Set start time for Temporal determinism
        if (task.startTime == 0L) {
            task.startTime = Workflow.currentTimeMillis()
        }

        // Delegate to Conductor's system task implementation
        systemTaskExecutor.execute(workflowModel, task)

        // Set end time if task completed
        if (task.status.isTerminal && task.endTime == 0L) {
            task.endTime = Workflow.currentTimeMillis()
        }

        // Handle TERMINATE task specially - it affects workflow status
        // This is orchestration logic: Conductor's Terminate.execute() only sets task status,
        // we need to translate that to workflow status
        if (task.taskType == TaskType.TERMINATE.name && task.status == TaskModel.Status.COMPLETED) {
            val terminationStatus = task.inputData["terminationStatus"] as? String
            workflowModel.status = when (terminationStatus) {
                "COMPLETED" -> WorkflowModel.Status.COMPLETED
                "FAILED" -> WorkflowModel.Status.FAILED
                else -> WorkflowModel.Status.TERMINATED
            }
        }

        // Record output
        if (task.outputData != null) {
            taskOutputs[task.referenceTaskName] = task.outputData
        }

        logger.debug("System task {} completed with status: {}", task.referenceTaskName, task.status)
    }

    /**
     * Re-evaluate IN_PROGRESS system tasks that may now be able to complete.
     * This includes both async tasks (like JOIN) and tasks that need multiple
     * execute() calls (like DO_WHILE).
     */
    private fun reEvaluateInProgressSystemTasks() {
        workflowModel.tasks
            .filter { isSystemTask(it) && it.status == TaskModel.Status.IN_PROGRESS }
            .forEach { task ->
                logger.debug("Re-evaluating IN_PROGRESS system task: {}", task.referenceTaskName)
                executeSystemTask(task)
            }
    }

    /**
     * Execute any tasks that were scheduled but not yet executed.
     * This handles tasks added by system tasks like DO_WHILE's scheduleLoopTasks.
     * Loops until no more tasks need to be executed (handles nested scheduling).
     */
    private fun executeNewlyScheduledTasks() {
        var iterations = 0
        val maxIterations = 100  // Safety limit to prevent infinite loops

        while (iterations < maxIterations) {
            val scheduledTasks = workflowModel.tasks.filter {
                it.status == TaskModel.Status.SCHEDULED
            }

            if (scheduledTasks.isEmpty()) break

            iterations++
            logger.debug("Executing {} newly scheduled tasks (iteration {})", scheduledTasks.size, iterations)

            // Separate system tasks from worker tasks
            val (systemTasks, workerTasks) = scheduledTasks.partition { isSystemTask(it) }

            // Execute sync system tasks
            systemTasks.filter { !systemTaskExecutor.isAsync(it.taskType) }
                .forEach { executeSystemTask(it) }

            // Execute worker tasks
            executeWorkerTasks(workerTasks)

            // Execute async system tasks
            systemTasks.filter { systemTaskExecutor.isAsync(it.taskType) }
                .forEach { executeSystemTask(it) }

            // After executing tasks, re-evaluate IN_PROGRESS system tasks
            // This allows DO_WHILE to check if the loop should continue or complete
            reEvaluateInProgressSystemTasks()
        }
    }

    private fun executeWorkerTasks(tasks: List<TaskModel>) {
        if (tasks.isEmpty()) return

        // Check if tasks can run in parallel (FORK branches)
        val canParallel = tasks.size > 1 && tasks.all { isForkBranchTask(it) }

        if (canParallel) {
            executeTasksInParallel(tasks)
        } else {
            tasks.forEach { executeWorkerTask(it) }
        }
    }

    private fun isForkBranchTask(task: TaskModel): Boolean {
        return workflowModel.tasks.any { t ->
            t.taskType == TaskType.FORK_JOIN.name && t.status != TaskModel.Status.COMPLETED
        }
    }

    /**
     * Execute a worker task via Temporal activity.
     */
    private fun executeWorkerTask(task: TaskModel) {
        logger.debug("Executing worker task: {} ({})", task.referenceTaskName, task.taskDefName)

        task.status = TaskModel.Status.IN_PROGRESS
        task.startTime = Workflow.currentTimeMillis()

        try {
            val result = activities.executeTask(
                taskName = task.taskDefName,
                taskRefName = task.referenceTaskName,
                input = task.inputData ?: emptyMap()
            )

            task.outputData = result.output
            task.status = if (result.status == "COMPLETED") {
                TaskModel.Status.COMPLETED
            } else {
                TaskModel.Status.FAILED
            }
            if (result.failureReason != null) {
                task.reasonForIncompletion = result.failureReason
            }
        } catch (e: Exception) {
            logger.error("Task execution failed: {}", task.referenceTaskName, e)
            task.status = TaskModel.Status.FAILED
            task.reasonForIncompletion = e.message
        }

        task.endTime = Workflow.currentTimeMillis()
        taskOutputs[task.referenceTaskName] = task.outputData ?: emptyMap()
    }

    private fun executeTasksInParallel(tasks: List<TaskModel>) {
        logger.debug("Executing {} tasks in parallel", tasks.size)

        val promises = tasks.map { task ->
            Async.function { executeWorkerTask(task) }
        }

        Promise.allOf(promises).get()
    }

    private fun updateTaskInWorkflow(task: TaskModel) {
        val existingIndex = workflowModel.tasks.indexOfFirst { it.taskId == task.taskId }
        if (existingIndex >= 0) {
            workflowModel.tasks[existingIndex] = task
        }
    }

    private fun processSignals() {
        pendingSignals.forEach { (taskRefName, output) ->
            val task = workflowModel.tasks.find { it.referenceTaskName == taskRefName }
            if (task != null && task.status == TaskModel.Status.IN_PROGRESS) {
                task.outputData = output
                task.status = TaskModel.Status.COMPLETED
                task.endTime = Workflow.currentTimeMillis()
                taskOutputs[taskRefName] = output
            }
        }
        pendingSignals.clear()
    }

    override fun completeTask(taskRefName: String, output: Map<String, Any>) {
        logger.info("Received signal to complete task: {}", taskRefName)
        pendingSignals[taskRefName] = output
    }

    override fun getStatus(): WorkflowStatusInfo {
        val completed = workflowModel.tasks.filter {
            it.status == TaskModel.Status.COMPLETED
        }.map { it.referenceTaskName }

        val pending = workflowModel.tasks.filter {
            it.status == TaskModel.Status.SCHEDULED || it.status == TaskModel.Status.IN_PROGRESS
        }.map { it.referenceTaskName }

        val failed = workflowModel.tasks.filter {
            it.status == TaskModel.Status.FAILED
        }.map { it.referenceTaskName }

        return WorkflowStatusInfo(
            workflowStatus = workflowModel.status.name,
            completedTasks = completed,
            pendingTasks = pending,
            failedTasks = failed
        )
    }

    /**
     * Populate TaskDefinition on each WorkflowTask.
     * Required because SimpleTaskMapper expects taskDefinition to be set.
     */
    private fun populateTaskDefinitions(workflowDef: WorkflowDef, taskDefsJson: Map<String, String>) {
        val taskDefs = taskDefsJson.mapValues { (_, json) ->
            objectMapper.readValue<TaskDef>(json)
        }

        fun populateTask(task: WorkflowTask) {
            if (task.taskDefinition == null && taskDefs.containsKey(task.name)) {
                task.taskDefinition = taskDefs[task.name]
            }
            task.decisionCases?.values?.flatten()?.forEach { populateTask(it) }
            task.defaultCase?.forEach { populateTask(it) }
            task.forkTasks?.flatten()?.forEach { populateTask(it) }
            task.loopOver?.forEach { populateTask(it) }
        }

        workflowDef.tasks.forEach { populateTask(it) }
    }

    /**
     * Disable Conductor timeouts to use Temporal timeouts instead.
     */
    private fun disableConductorTimeouts(workflowDef: WorkflowDef) {
        workflowDef.timeoutSeconds = 0

        fun disableTaskTimeouts(task: WorkflowTask) {
            task.taskDefinition?.let { taskDef ->
                taskDef.timeoutSeconds = 0
                taskDef.pollTimeoutSeconds = 0
                taskDef.responseTimeoutSeconds = 0
            }
            task.decisionCases?.values?.flatten()?.forEach { disableTaskTimeouts(it) }
            task.defaultCase?.forEach { disableTaskTimeouts(it) }
            task.forkTasks?.flatten()?.forEach { disableTaskTimeouts(it) }
            task.loopOver?.forEach { disableTaskTimeouts(it) }
        }

        workflowDef.tasks.forEach { disableTaskTimeouts(it) }
    }
}
