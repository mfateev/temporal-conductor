package io.temporal.conductor.ext

import com.netflix.conductor.common.metadata.tasks.TaskResult
import com.netflix.conductor.common.metadata.workflow.RerunWorkflowRequest
import com.netflix.conductor.common.metadata.workflow.SkipTaskRequest
import com.netflix.conductor.common.run.Workflow
import com.netflix.conductor.common.utils.TaskUtils
import com.netflix.conductor.core.execution.DeciderService
import com.netflix.conductor.core.execution.StartWorkflowInput
import com.netflix.conductor.core.execution.WorkflowExecutor
import com.netflix.conductor.model.TaskModel
import com.netflix.conductor.model.WorkflowModel
import io.temporal.workflow.Workflow as TemporalWorkflow
import org.slf4j.LoggerFactory

/**
 * In-memory WorkflowExecutor for use inside Temporal workflows.
 *
 * This implementation keeps all state in the WorkflowModel, which Temporal
 * makes durable through its replay mechanism. No external database persistence
 * is needed.
 *
 * Key method:
 * - scheduleNextIteration(): Used by DoWhile to schedule the next loop iteration
 *
 * Design principles:
 * - All state lives in WorkflowModel (Temporal makes this durable)
 * - Uses Workflow.currentTimeMillis() for deterministic timestamps
 * - Delegates task scheduling to DeciderService
 * - No external database or queue interactions
 */
class InMemoryWorkflowExecutor(
    private val deciderService: DeciderService
) : WorkflowExecutor {

    private val logger = LoggerFactory.getLogger(InMemoryWorkflowExecutor::class.java)

    /**
     * Schedule the next iteration of a DO_WHILE loop.
     *
     * This is the key method needed by Conductor's DoWhile system task.
     * It schedules the first task in the loopOver list for the current iteration.
     * The DeciderService will handle scheduling the rest when this task completes.
     */
    override fun scheduleNextIteration(loopTask: TaskModel, workflow: WorkflowModel) {
        val loopOverTasks = loopTask.workflowTask?.loopOver
        if (loopOverTasks.isNullOrEmpty()) {
            logger.warn("No loopOver tasks defined for DO_WHILE task: {}", loopTask.referenceTaskName)
            return
        }

        logger.debug(
            "Scheduling next iteration {} for DO_WHILE task: {}",
            loopTask.iteration,
            loopTask.referenceTaskName
        )

        // Use DeciderService to create task models for the first loopOver task
        val tasksToSchedule = deciderService.getTasksToBeScheduled(
            workflow,
            loopOverTasks[0],
            loopTask.retryCount,
            null
        )

        // Append iteration number to task reference names and set iteration
        tasksToSchedule.forEach { task ->
            task.referenceTaskName = TaskUtils.appendIteration(
                task.referenceTaskName,
                loopTask.iteration
            )
            task.iteration = loopTask.iteration
            task.scheduledTime = TemporalWorkflow.currentTimeMillis()
            task.status = TaskModel.Status.SCHEDULED
        }

        // Add tasks to workflow model (Temporal makes this durable)
        workflow.tasks.addAll(tasksToSchedule)

        logger.debug(
            "Scheduled {} tasks for iteration {} of DO_WHILE task: {}",
            tasksToSchedule.size,
            loopTask.iteration,
            loopTask.referenceTaskName
        )
    }

    // ===== Methods not needed for system tasks - no-op or throw =====

    override fun resetCallbacksForWorkflow(workflowId: String) {
        // No-op: Temporal handles callbacks differently
    }

    override fun rerun(request: RerunWorkflowRequest): String {
        throw UnsupportedOperationException("Use Temporal workflow retry instead")
    }

    override fun restart(workflowId: String, useLatestDefinitions: Boolean) {
        throw UnsupportedOperationException("Use Temporal workflow restart instead")
    }

    override fun retry(workflowId: String, resumeSubworkflowTasks: Boolean) {
        throw UnsupportedOperationException("Use Temporal workflow retry instead")
    }

    override fun updateTask(taskResult: TaskResult): TaskModel {
        throw UnsupportedOperationException("Tasks are updated directly in workflow code")
    }

    override fun getTask(taskId: String): TaskModel? = null

    override fun getRunningWorkflows(workflowName: String, version: Int): List<Workflow> = emptyList()

    override fun getWorkflows(name: String, version: Int?, startTime: Long?, endTime: Long?): List<String> = emptyList()

    override fun getRunningWorkflowIds(workflowName: String, version: Int): List<String> = emptyList()

    override fun decide(workflowId: String): WorkflowModel {
        throw UnsupportedOperationException("Use deciderService.decide() directly in workflow code")
    }

    override fun decideWithLock(workflow: WorkflowModel): WorkflowModel? = null

    override fun terminateWorkflow(workflowId: String, reason: String) {
        // Handled by workflow code setting status directly
    }

    override fun terminateWorkflow(workflow: WorkflowModel, reason: String, failureWorkflow: String?): WorkflowModel {
        workflow.status = WorkflowModel.Status.TERMINATED
        workflow.reasonForIncompletion = reason
        return workflow
    }

    override fun pauseWorkflow(workflowId: String) {
        // Not supported in Temporal context
    }

    override fun resumeWorkflow(workflowId: String) {
        // Not supported in Temporal context
    }

    override fun skipTaskFromWorkflow(workflowId: String, taskReferenceName: String, skipTaskRequest: SkipTaskRequest) {
        throw UnsupportedOperationException("Task skipping not supported in Temporal context")
    }

    override fun getWorkflow(workflowId: String, includeTasks: Boolean): WorkflowModel? = null

    override fun startWorkflow(input: StartWorkflowInput): String {
        throw UnsupportedOperationException("Use Temporal child workflow instead")
    }
}
