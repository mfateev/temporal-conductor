/*
 * Copyright Temporal Technologies Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.temporal.conductor.workflow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.core.exception.TerminateWorkflowException;
import com.netflix.conductor.core.execution.DeciderOutcomeAccessor;
import com.netflix.conductor.core.execution.DeciderService;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.conductor.activity.TaskExecutionActivities;
import io.temporal.conductor.executor.InMemoryMetadataDAO;
import io.temporal.conductor.executor.SystemTaskExecutor;
import io.temporal.conductor.executor.TemporalDeciderServiceFactory;
import io.temporal.common.converter.EncodedValues;
import io.temporal.conductor.workflow.model.ConductorWorkflowInput;
import io.temporal.conductor.workflow.model.ConductorWorkflowOutput;
import io.temporal.conductor.workflow.model.TaskExecutionResult;
import io.temporal.conductor.workflow.model.TaskState;
import io.temporal.conductor.workflow.model.WorkflowState;
import io.temporal.workflow.Async;
import io.temporal.workflow.DynamicQueryHandler;
import io.temporal.workflow.DynamicSignalHandler;
import io.temporal.workflow.DynamicWorkflow;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;

/**
 * Dynamic workflow implementation that runs Conductor's DeciderService
 * within Temporal's workflow execution model.
 *
 * <p>This implements {@link DynamicWorkflow} so that the Temporal workflow type
 * matches the Conductor workflow name (e.g., "hello_workflow" instead of "ConductorWorkflow").
 *
 * <p>Architecture:
 * <ul>
 *   <li>DeciderService: Handles workflow scheduling decisions (what tasks to run next)</li>
 *   <li>SystemTaskExecutor: Delegates to Conductor's native system task implementations</li>
 *   <li>Temporal Activities: Execute worker tasks via Temporal's durable execution</li>
 * </ul>
 *
 * <p>This implementation focuses on orchestration, delegating task execution logic
 * to Conductor's components.
 */
public class ConductorWorkflowImpl implements DynamicWorkflow {

    private static final Logger logger = Workflow.getLogger(ConductorWorkflowImpl.class);
    private static final int MAX_ITERATIONS = 1000;

    private final ObjectMapper objectMapper = new ObjectMapper();

    // Async execution tracking - for event-driven scheduling loop
    private final Map<String, Promise<TaskExecutionResult>> activityPromises = new HashMap<>();
    private final Set<String> pendingTaskIds = new HashSet<>();

    // WAIT task timers - maps task reference name to timer promise
    private final Map<String, Promise<Void>> waitTimers = new HashMap<>();

    // Conductor components
    private WorkflowModel workflowModel;
    private DeciderService deciderService;
    private InMemoryMetadataDAO metadataDao;
    private SystemTaskExecutor systemTaskExecutor;

    // Workflow state
    private final Map<String, Map<String, Object>> taskOutputs = new HashMap<>();
    private final Map<String, Map<String, Object>> pendingSignals = new HashMap<>();
    private volatile boolean isPaused = false;
    private long lastUpdateTime = 0;

    // Activity stub for worker task execution
    private final TaskExecutionActivities activities = Workflow.newActivityStub(
            TaskExecutionActivities.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofMinutes(10))
                    .setRetryOptions(
                            RetryOptions.newBuilder()
                                    .setMaximumAttempts(3)
                                    .build()
                    )
                    .build()
    );

    @Override
    public Object execute(EncodedValues args) {
        // Decode input from EncodedValues
        ConductorWorkflowInput input = args.get(0, ConductorWorkflowInput.class);

        String workflowRunId = Workflow.getInfo().getRunId();
        String workflowId = Workflow.getInfo().getWorkflowId();
        String workflowType = Workflow.getInfo().getWorkflowType();

        logger.info("Starting Conductor workflow execution, type: {}, runId: {}", workflowType, workflowRunId);

        // Register dynamic signal handlers
        registerSignalHandlers();

        // Register dynamic query handlers
        registerQueryHandlers();

        // Initialize Conductor components
        initializeConductorComponents(input, workflowRunId, workflowId);

        // Initialize search attributes
        WorkflowDef workflowDef = workflowModel.getWorkflowDefinition();
        initializeSearchAttributes(workflowDef, input);

        // Main scheduling loop
        try {
            runSchedulingLoop();
        } catch (TerminateWorkflowException e) {
            handleTerminateException(e);
        } catch (Exception e) {
            logger.error("Workflow execution failed", e);
            workflowModel.setStatus(WorkflowModel.Status.FAILED);
            workflowModel.setReasonForIncompletion(e.getMessage());
        }

        // Update final status
        updateStatusAttribute(workflowModel.getStatus());

        // Update failed task names if any
        updateFailedTaskNamesAttribute();

        return new ConductorWorkflowOutput(
                workflowModel.getStatus().name(),
                workflowModel.getOutput() != null ? workflowModel.getOutput() : Collections.emptyMap(),
                workflowModel.getReasonForIncompletion(),
                new HashMap<>(taskOutputs)
        );
    }

    private void initializeConductorComponents(
            ConductorWorkflowInput input,
            String workflowRunId,
            String workflowId) {

        // Initialize metadata DAO with task definitions
        metadataDao = new InMemoryMetadataDAO();
        for (Map.Entry<String, String> entry : input.getTaskDefsJson().entrySet()) {
            try {
                TaskDef taskDef = objectMapper.readValue(entry.getValue(), TaskDef.class);
                metadataDao.createTaskDef(taskDef);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to parse task definition: " + entry.getKey(), e);
            }
        }

        // Create DeciderService with deterministic ID generation
        TemporalDeciderServiceFactory factory =
                TemporalDeciderServiceFactory.forTemporalWorkflow(metadataDao, workflowRunId);
        deciderService = factory.create();

        // Create SystemTaskExecutor with DeciderService for system task execution
        systemTaskExecutor = new SystemTaskExecutor(deciderService, objectMapper);

        // Parse and preprocess workflow definition
        WorkflowDef workflowDef;
        try {
            workflowDef = objectMapper.readValue(input.getWorkflowDefJson(), WorkflowDef.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse workflow definition", e);
        }
        populateTaskDefinitions(workflowDef, input.getTaskDefsJson());
        disableConductorTimeouts(workflowDef);

        // Initialize workflow model
        workflowModel = new WorkflowModel();
        workflowModel.setWorkflowId(workflowId);
        workflowModel.setWorkflowDefinition(workflowDef);
        workflowModel.setStatus(WorkflowModel.Status.RUNNING);
        workflowModel.setInput(input.getWorkflowInput());
        long currentTime = Workflow.currentTimeMillis();
        workflowModel.setCreateTime(currentTime);
        lastUpdateTime = currentTime;
        workflowModel.setCorrelationId(input.getCorrelationId());
        workflowModel.setPriority(input.getPriority() != null ? input.getPriority() : 0);
        workflowModel.setOwnerApp(input.getOwnerApp());
        workflowModel.setCreatedBy(input.getCreatedBy());
    }

    private void initializeSearchAttributes(WorkflowDef workflowDef, ConductorWorkflowInput input) {
        Workflow.upsertTypedSearchAttributes(
                ConductorSearchAttributes.WORKFLOW_TYPE.valueSet(workflowDef.getName()),
                ConductorSearchAttributes.WORKFLOW_VERSION.valueSet((long) workflowDef.getVersion()),
                ConductorSearchAttributes.STATUS.valueSet("RUNNING"),
                ConductorSearchAttributes.CORRELATION_ID.valueSet(
                        input.getCorrelationId() != null ? input.getCorrelationId() : ""),
                ConductorSearchAttributes.PRIORITY.valueSet(
                        input.getPriority() != null ? input.getPriority().longValue() : 0L),
                ConductorSearchAttributes.OWNER_APP.valueSet(
                        input.getOwnerApp() != null ? input.getOwnerApp() : "")
        );
    }

    private void updateStatusAttribute(WorkflowModel.Status status) {
        Workflow.upsertTypedSearchAttributes(
                ConductorSearchAttributes.STATUS.valueSet(status.name())
        );
    }

    private void updateFailedTaskNamesAttribute() {
        List<String> failedTaskNames = workflowModel.getTasks().stream()
                .filter(t -> t.getStatus() == TaskModel.Status.FAILED
                        || t.getStatus() == TaskModel.Status.FAILED_WITH_TERMINAL_ERROR)
                .map(TaskModel::getReferenceTaskName)
                .collect(Collectors.toList());

        if (!failedTaskNames.isEmpty()) {
            Workflow.upsertTypedSearchAttributes(
                    ConductorSearchAttributes.FAILED_TASK_NAMES.valueSet(failedTaskNames)
            );
        }
    }

    private void handleTerminateException(TerminateWorkflowException e) {
        String message = e.getMessage();
        if (message != null && message.contains("No tasks found")) {
            logger.info("Empty workflow completed (no tasks to execute)");
            workflowModel.setStatus(WorkflowModel.Status.COMPLETED);
        } else {
            logger.error("Workflow terminated: {}", message);
            workflowModel.setStatus(WorkflowModel.Status.TERMINATED);
            workflowModel.setReasonForIncompletion(message);
        }
    }

    private void runSchedulingLoop() {
        int iterations = 0;

        while (!workflowModel.getStatus().isTerminal() && iterations < MAX_ITERATIONS) {
            iterations++;
            logger.info("Scheduling loop iteration {}", iterations);

            // Check for pause
            if (isPaused) {
                Workflow.await(() -> !isPaused);
                continue;
            }

            // Get scheduling decision from DeciderService
            DeciderService.DeciderOutcome outcome = deciderService.decide(workflowModel);

            if (DeciderOutcomeAccessor.isComplete(outcome)) {
                completeWorkflow();
                break;
            }

            // Apply task updates from DeciderService
            List<TaskModel> tasksToUpdate = DeciderOutcomeAccessor.getTasksToBeUpdated(outcome);
            for (TaskModel task : tasksToUpdate) {
                updateTaskInWorkflow(task);
            }

            List<TaskModel> tasksToSchedule = DeciderOutcomeAccessor.getTasksToBeScheduled(outcome);
            logger.info("tasksToSchedule count={}, tasksToUpdate count={}",
                    tasksToSchedule.size(), tasksToUpdate.size());

            if (tasksToSchedule.isEmpty()) {
                // Re-evaluate IN_PROGRESS system tasks (JOIN, DO_WHILE) even when no new tasks
                // This allows JOIN to complete when all its branches finish
                reEvaluateInProgressSystemTasks();

                // Check again after re-evaluation - new tasks may have been scheduled
                executeNewlyScheduledTasks();

                // Don't check shouldCompleteWorkflow() here - let DeciderService determine
                // completion on the next iteration. DeciderOutcomeAccessor.isComplete(outcome)
                // at the start of the loop is the authoritative completion check.
                // This ensures tasks after JOIN (like final_task) get scheduled.
                waitForSignalsOrChanges();
                continue;
            }

            // Add tasks to workflow model
            for (TaskModel task : tasksToSchedule) {
                task.setWorkflowInstanceId(workflowModel.getWorkflowId());
                workflowModel.getTasks().add(task);
            }

            // Execute tasks
            executeTasks(tasksToSchedule);

            // Re-evaluate IN_PROGRESS system tasks (JOIN, DO_WHILE, etc.)
            reEvaluateInProgressSystemTasks();

            // Execute any tasks scheduled by system task re-evaluation
            executeNewlyScheduledTasks();
        }

        if (iterations >= MAX_ITERATIONS) {
            logger.error("Scheduling loop exceeded max iterations");
            workflowModel.setStatus(WorkflowModel.Status.FAILED);
            workflowModel.setReasonForIncompletion("Scheduling loop exceeded max iterations");
        }
    }

    private boolean shouldCompleteWorkflow() {
        boolean allTasksCompleted = workflowModel.getTasks().stream()
                .allMatch(t -> t.getStatus() == TaskModel.Status.COMPLETED
                        || t.getStatus() == TaskModel.Status.SKIPPED);

        boolean hasAsyncSystemTasksInProgress = workflowModel.getTasks().stream()
                .anyMatch(t -> systemTaskExecutor.isAsync(t.getTaskType())
                        && t.getStatus() == TaskModel.Status.IN_PROGRESS);

        return allTasksCompleted && !hasAsyncSystemTasksInProgress;
    }

    private void completeWorkflow() {
        logger.info("Workflow completed");
        workflowModel.setStatus(WorkflowModel.Status.COMPLETED);
        workflowModel.setEndTime(Workflow.currentTimeMillis());

        // Find last completed task to get output
        Optional<TaskModel> lastTask = workflowModel.getTasks().stream()
                .filter(t -> t.getStatus() == TaskModel.Status.COMPLETED)
                .reduce((first, second) -> second);

        if (lastTask.isPresent() && (workflowModel.getOutput() == null
                || workflowModel.getOutput().isEmpty())) {
            workflowModel.setOutput(lastTask.get().getOutputData());
        }
    }

    private void waitForSignalsOrChanges() {
        // Check if there are any IN_PROGRESS tasks that might be waiting for signals
        // (e.g., WAIT tasks without timeout)
        boolean hasInProgressWaitingTasks = workflowModel.getTasks().stream()
                .anyMatch(t -> t.getStatus() == TaskModel.Status.IN_PROGRESS);

        // If there are no pending activities, timers, signals, AND no IN_PROGRESS tasks,
        // don't wait - just return and let the scheduling loop continue
        if (activityPromises.isEmpty() && waitTimers.isEmpty() && pendingSignals.isEmpty()
                && !hasInProgressWaitingTasks) {
            logger.debug("No pending work to wait for, continuing to next iteration");
            return;
        }

        logger.debug("Waiting for events... (activities: {}, timers: {}, signals: {}, inProgress: {})",
                activityPromises.size(), waitTimers.size(), pendingSignals.size(), hasInProgressWaitingTasks);

        // Wait for ANY event: activity completion, signal, WAIT timer, or state change
        // This is event-driven - no polling
        Workflow.await(() ->
                anyActivityCompleted() ||
                anyWaitTimerCompleted() ||
                !pendingSignals.isEmpty()
        );

        // Process completed activities
        processCompletedActivities();

        // Process completed WAIT timers
        processCompletedWaitTimers();

        // Process signals
        if (!pendingSignals.isEmpty()) {
            processSignals();
        }
    }

    private boolean anyActivityCompleted() {
        for (Promise<?> p : activityPromises.values()) {
            if (p.isCompleted()) return true;
        }
        return false;
    }

    private boolean anyWaitTimerCompleted() {
        for (Promise<?> p : waitTimers.values()) {
            if (p.isCompleted()) return true;
        }
        return false;
    }

    private void processCompletedWaitTimers() {
        List<String> completedRefs = new ArrayList<>();

        for (Map.Entry<String, Promise<Void>> entry : waitTimers.entrySet()) {
            String taskRefName = entry.getKey();
            Promise<Void> timerPromise = entry.getValue();

            if (timerPromise.isCompleted()) {
                // Find the WAIT task and complete it
                Optional<TaskModel> taskOpt = workflowModel.getTasks().stream()
                        .filter(t -> t.getReferenceTaskName().equals(taskRefName))
                        .findFirst();

                if (taskOpt.isPresent()) {
                    TaskModel task = taskOpt.get();
                    if (task.getStatus() == TaskModel.Status.IN_PROGRESS) {
                        logger.debug("WAIT timer completed for task: {}", taskRefName);
                        task.setStatus(TaskModel.Status.COMPLETED);
                        task.setEndTime(Workflow.currentTimeMillis());
                        taskOutputs.put(taskRefName,
                                task.getOutputData() != null ? task.getOutputData() : Collections.emptyMap());
                    }
                }
                completedRefs.add(taskRefName);
            }
        }

        for (String ref : completedRefs) {
            waitTimers.remove(ref);
        }
    }

    private void processCompletedActivities() {
        List<String> completedTaskIds = new ArrayList<>();

        for (Map.Entry<String, Promise<TaskExecutionResult>> entry : activityPromises.entrySet()) {
            String taskId = entry.getKey();
            Promise<TaskExecutionResult> promise = entry.getValue();

            if (promise.isCompleted()) {
                TaskModel task = getTaskById(taskId);
                if (task != null) {
                    try {
                        TaskExecutionResult result = promise.get();
                        task.setOutputData(result.getOutput());
                        if ("COMPLETED".equals(result.getStatus())) {
                            task.setStatus(TaskModel.Status.COMPLETED);
                        } else {
                            task.setStatus(TaskModel.Status.FAILED);
                            task.setReasonForIncompletion(result.getFailureReason());
                        }
                    } catch (Exception e) {
                        task.setStatus(TaskModel.Status.FAILED);
                        task.setReasonForIncompletion(e.getMessage());
                    }
                    task.setEndTime(Workflow.currentTimeMillis());
                    taskOutputs.put(task.getReferenceTaskName(),
                            task.getOutputData() != null ? task.getOutputData() : Collections.emptyMap());
                }

                completedTaskIds.add(taskId);
                pendingTaskIds.remove(taskId);
            }
        }

        for (String taskId : completedTaskIds) {
            activityPromises.remove(taskId);
        }
    }

    private TaskModel getTaskById(String taskId) {
        return workflowModel.getTasks().stream()
                .filter(t -> t.getTaskId().equals(taskId))
                .findFirst()
                .orElse(null);
    }

    private void executeTasks(List<TaskModel> tasks) {
        // Track task IDs we're about to process
        Set<String> taskIdsToProcess = tasks.stream()
                .map(TaskModel::getTaskId)
                .collect(Collectors.toSet());

        // Separate system tasks from worker tasks
        List<TaskModel> systemTasks = new ArrayList<>();
        List<TaskModel> workerTasks = new ArrayList<>();

        for (TaskModel task : tasks) {
            boolean isSysTask = isSystemTask(task);
            logger.info("Task {} has type '{}', isSystemTask={}",
                    task.getReferenceTaskName(), task.getTaskType(), isSysTask);
            if (isSysTask) {
                systemTasks.add(task);
            } else {
                workerTasks.add(task);
            }
        }

        // Separate async system tasks (like JOIN) that need to wait for worker tasks
        List<TaskModel> asyncSystemTasks = new ArrayList<>();
        List<TaskModel> syncSystemTasks = new ArrayList<>();

        for (TaskModel task : systemTasks) {
            if (systemTaskExecutor.isAsync(task.getTaskType())) {
                asyncSystemTasks.add(task);
            } else {
                syncSystemTasks.add(task);
            }
        }

        // Execute sync system tasks first (FORK, SWITCH, etc.)
        for (TaskModel task : syncSystemTasks) {
            executeSystemTask(task);
        }

        // Check for tasks added by system tasks (e.g., DO_WHILE loop body tasks)
        List<TaskModel> newlyScheduledTasks = workflowModel.getTasks().stream()
                .filter(t -> t.getStatus() == TaskModel.Status.SCHEDULED
                        && !taskIdsToProcess.contains(t.getTaskId()))
                .collect(Collectors.toList());

        // Combine original worker tasks with newly scheduled tasks
        List<TaskModel> allWorkerTasks = new ArrayList<>(workerTasks);
        List<TaskModel> newSystemTasks = new ArrayList<>();

        for (TaskModel task : newlyScheduledTasks) {
            if (isSystemTask(task) && !systemTaskExecutor.isAsync(task.getTaskType())) {
                newSystemTasks.add(task);
            } else if (!isSystemTask(task)) {
                allWorkerTasks.add(task);
            }
        }

        // Execute any new sync system tasks that were added
        for (TaskModel task : newSystemTasks) {
            executeSystemTask(task);
        }

        // Execute worker tasks
        executeWorkerTasks(allWorkerTasks);

        // Execute async system tasks after worker tasks (JOIN)
        for (TaskModel task : asyncSystemTasks) {
            executeSystemTask(task);
        }
    }

    private boolean isSystemTask(TaskModel task) {
        return SystemTaskExecutor.isKnownSystemTaskType(task.getTaskType());
    }

    /**
     * Execute a system task using Conductor's native implementation.
     */
    private void executeSystemTask(TaskModel task) {
        logger.debug("Executing system task: {} ({})",
                task.getReferenceTaskName(), task.getTaskType());

        // Set start time for Temporal determinism
        if (task.getStartTime() == 0L) {
            task.setStartTime(Workflow.currentTimeMillis());
        }

        // Handle WAIT task specially - use Temporal timers for deterministic timeout
        if (TaskType.WAIT.name().equals(task.getTaskType())) {
            executeWaitTask(task);
            return;
        }

        // Delegate to Conductor's system task implementation
        systemTaskExecutor.execute(workflowModel, task);

        // Set end time if task completed
        if (task.getStatus().isTerminal() && task.getEndTime() == 0L) {
            task.setEndTime(Workflow.currentTimeMillis());
        }

        // Handle TERMINATE task specially
        if (TaskType.TERMINATE.name().equals(task.getTaskType())
                && task.getStatus() == TaskModel.Status.COMPLETED) {
            Object terminationStatus = task.getInputData().get("terminationStatus");
            if ("COMPLETED".equals(terminationStatus)) {
                workflowModel.setStatus(WorkflowModel.Status.COMPLETED);
            } else if ("FAILED".equals(terminationStatus)) {
                workflowModel.setStatus(WorkflowModel.Status.FAILED);
            } else {
                workflowModel.setStatus(WorkflowModel.Status.TERMINATED);
            }
        }

        // Record output
        if (task.getOutputData() != null) {
            taskOutputs.put(task.getReferenceTaskName(), task.getOutputData());
        }

        logger.debug("System task {} completed with status: {}",
                task.getReferenceTaskName(), task.getStatus());
    }

    /**
     * Execute WAIT task using Temporal timers for deterministic timeout handling.
     * WAIT tasks wait for either a timeout or a signal to complete.
     *
     * Note: The WaitTaskMapper uses System.currentTimeMillis() which is non-deterministic.
     * We parse the duration/until input directly and use Workflow.currentTimeMillis() for
     * deterministic timer handling.
     */
    private void executeWaitTask(TaskModel task) {
        String taskRefName = task.getReferenceTaskName();

        // WAIT tasks come as IN_PROGRESS from WaitTaskMapper (not SCHEDULED)
        // Ensure status is IN_PROGRESS
        if (task.getStatus() == TaskModel.Status.SCHEDULED) {
            task.setStatus(TaskModel.Status.IN_PROGRESS);
        }

        // If already completed (e.g., by signal), we're done
        if (task.getStatus().isTerminal()) {
            if (task.getEndTime() == 0L) {
                task.setEndTime(Workflow.currentTimeMillis());
            }
            taskOutputs.put(taskRefName,
                    task.getOutputData() != null ? task.getOutputData() : Collections.emptyMap());
            return;
        }

        // Check if timer already exists
        if (waitTimers.containsKey(taskRefName)) {
            // Timer already running - check if completed
            Promise<Void> existingTimer = waitTimers.get(taskRefName);
            if (existingTimer.isCompleted()) {
                task.setStatus(TaskModel.Status.COMPLETED);
                task.setEndTime(Workflow.currentTimeMillis());
                waitTimers.remove(taskRefName);
                taskOutputs.put(taskRefName,
                        task.getOutputData() != null ? task.getOutputData() : Collections.emptyMap());
                logger.debug("WAIT task {} completed by timer", taskRefName);
            }
            return;
        }

        // Parse duration from input data (ignore waitTimeout set by mapper - it uses non-deterministic time)
        Map<String, Object> inputData = task.getInputData();
        if (inputData != null) {
            String durationStr = inputData.get("duration") != null ? inputData.get("duration").toString() : null;
            String untilStr = inputData.get("until") != null ? inputData.get("until").toString() : null;

            if (durationStr != null && !durationStr.isEmpty()) {
                // Parse duration string (e.g., "100ms", "1s", "5m")
                Duration duration = parseDurationString(durationStr);
                if (duration != null && !duration.isZero() && !duration.isNegative()) {
                    Promise<Void> timer = Workflow.newTimer(duration);
                    waitTimers.put(taskRefName, timer);
                    logger.debug("Started WAIT timer for task {} with duration {}", taskRefName, duration);
                    return;
                }
            } else if (untilStr != null && !untilStr.isEmpty()) {
                // For 'until', we'd need to parse the date and calculate delay
                // For now, use the pre-calculated waitTimeout as a fallback
                long waitTimeout = task.getWaitTimeout();
                if (waitTimeout > 0) {
                    long currentTime = Workflow.currentTimeMillis();
                    long delayMs = waitTimeout - currentTime;
                    if (delayMs > 0) {
                        Promise<Void> timer = Workflow.newTimer(Duration.ofMillis(delayMs));
                        waitTimers.put(taskRefName, timer);
                        logger.debug("Started WAIT timer for task {} with delay {}ms (until mode)", taskRefName, delayMs);
                    } else {
                        // Already past the 'until' time
                        task.setStatus(TaskModel.Status.COMPLETED);
                        task.setEndTime(Workflow.currentTimeMillis());
                        taskOutputs.put(taskRefName,
                                task.getOutputData() != null ? task.getOutputData() : Collections.emptyMap());
                    }
                    return;
                }
            }
        }
        // No timeout specified - WAIT task waits indefinitely for a signal
        logger.debug("WAIT task {} waiting indefinitely for signal", taskRefName);
    }

    /**
     * Parse a duration string like "100ms", "1s", "5m", "1h", "2d".
     */
    private Duration parseDurationString(String durationStr) {
        if (durationStr == null || durationStr.isEmpty()) {
            return null;
        }

        try {
            // Try ISO-8601 duration format first (PT1S, PT5M, etc.)
            if (durationStr.startsWith("P") || durationStr.startsWith("p")) {
                return Duration.parse(durationStr);
            }

            // Parse human-readable format (100ms, 1s, 5m, 1h, 2d)
            durationStr = durationStr.trim().toLowerCase();
            if (durationStr.endsWith("ms")) {
                return Duration.ofMillis(Long.parseLong(durationStr.substring(0, durationStr.length() - 2)));
            } else if (durationStr.endsWith("s")) {
                return Duration.ofSeconds(Long.parseLong(durationStr.substring(0, durationStr.length() - 1)));
            } else if (durationStr.endsWith("m")) {
                return Duration.ofMinutes(Long.parseLong(durationStr.substring(0, durationStr.length() - 1)));
            } else if (durationStr.endsWith("h")) {
                return Duration.ofHours(Long.parseLong(durationStr.substring(0, durationStr.length() - 1)));
            } else if (durationStr.endsWith("d")) {
                return Duration.ofDays(Long.parseLong(durationStr.substring(0, durationStr.length() - 1)));
            } else {
                // Assume seconds if no unit
                return Duration.ofSeconds(Long.parseLong(durationStr));
            }
        } catch (Exception e) {
            logger.warn("Failed to parse duration string: {}", durationStr, e);
            return null;
        }
    }

    /**
     * Re-evaluate IN_PROGRESS system tasks that may now be able to complete.
     */
    private void reEvaluateInProgressSystemTasks() {
        List<TaskModel> inProgressSystemTasks = workflowModel.getTasks().stream()
                .filter(t -> isSystemTask(t) && t.getStatus() == TaskModel.Status.IN_PROGRESS)
                .collect(Collectors.toList());

        for (TaskModel task : inProgressSystemTasks) {
            logger.debug("Re-evaluating IN_PROGRESS system task: {}", task.getReferenceTaskName());
            executeSystemTask(task);
        }
    }

    /**
     * Execute any tasks that were scheduled but not yet executed.
     */
    private void executeNewlyScheduledTasks() {
        int iterations = 0;
        int maxIterations = 100;

        while (iterations < maxIterations) {
            List<TaskModel> scheduledTasks = workflowModel.getTasks().stream()
                    .filter(t -> t.getStatus() == TaskModel.Status.SCHEDULED)
                    .collect(Collectors.toList());

            if (scheduledTasks.isEmpty()) {
                break;
            }

            iterations++;
            logger.debug("Executing {} newly scheduled tasks (iteration {})",
                    scheduledTasks.size(), iterations);

            // Separate system tasks from worker tasks
            List<TaskModel> systemTasks = new ArrayList<>();
            List<TaskModel> workerTasks = new ArrayList<>();

            for (TaskModel task : scheduledTasks) {
                if (isSystemTask(task)) {
                    systemTasks.add(task);
                } else {
                    workerTasks.add(task);
                }
            }

            // Execute sync system tasks
            for (TaskModel task : systemTasks) {
                if (!systemTaskExecutor.isAsync(task.getTaskType())) {
                    executeSystemTask(task);
                }
            }

            // Execute worker tasks
            executeWorkerTasks(workerTasks);

            // Execute async system tasks
            for (TaskModel task : systemTasks) {
                if (systemTaskExecutor.isAsync(task.getTaskType())) {
                    executeSystemTask(task);
                }
            }

            // Re-evaluate IN_PROGRESS system tasks
            reEvaluateInProgressSystemTasks();
        }
    }

    private void executeWorkerTasks(List<TaskModel> tasks) {
        if (tasks.isEmpty()) {
            return;
        }

        // Check if tasks can run in parallel (FORK branches)
        boolean canParallel = tasks.size() > 1 && hasPendingForkJoin();

        if (canParallel) {
            // Start all tasks asynchronously for true Temporal parallelism
            logger.info("Executing {} tasks in parallel (FORK branches)", tasks.size());
            for (TaskModel task : tasks) {
                startWorkerTaskAsync(task);
            }
            // Don't block - let the scheduling loop handle completion
        } else {
            // Sequential execution - start task async but wait for completion
            for (TaskModel task : tasks) {
                executeWorkerTaskSync(task);
            }
        }
    }

    private boolean hasPendingForkJoin() {
        // Check for a pending JOIN task - this indicates we're in FORK branches
        // and tasks should be executed in parallel
        return workflowModel.getTasks().stream()
                .anyMatch(t -> TaskType.JOIN.name().equals(t.getTaskType())
                        && t.getStatus() != TaskModel.Status.COMPLETED);
    }

    /**
     * Start a worker task asynchronously - returns immediately.
     * The task completion will be handled by processCompletedActivities().
     */
    private void startWorkerTaskAsync(TaskModel task) {
        logger.debug("Starting async worker task: {} ({})",
                task.getReferenceTaskName(), task.getTaskDefName());

        task.setStatus(TaskModel.Status.IN_PROGRESS);
        task.setStartTime(Workflow.currentTimeMillis());

        // Start activity asynchronously
        Promise<TaskExecutionResult> promise = Async.function(
                activities::executeTask,
                task.getTaskDefName(),
                task.getReferenceTaskName(),
                task.getInputData() != null ? task.getInputData() : Collections.emptyMap()
        );

        // Track the promise for later completion handling
        activityPromises.put(task.getTaskId(), promise);
        pendingTaskIds.add(task.getTaskId());
    }

    /**
     * Execute a worker task synchronously - blocks until completion.
     */
    private void executeWorkerTaskSync(TaskModel task) {
        logger.debug("Executing sync worker task: {} ({})",
                task.getReferenceTaskName(), task.getTaskDefName());

        task.setStatus(TaskModel.Status.IN_PROGRESS);
        task.setStartTime(Workflow.currentTimeMillis());

        try {
            TaskExecutionResult result = activities.executeTask(
                    task.getTaskDefName(),
                    task.getReferenceTaskName(),
                    task.getInputData() != null ? task.getInputData() : Collections.emptyMap()
            );

            task.setOutputData(result.getOutput());
            if ("COMPLETED".equals(result.getStatus())) {
                task.setStatus(TaskModel.Status.COMPLETED);
            } else {
                task.setStatus(TaskModel.Status.FAILED);
            }
            if (result.getFailureReason() != null) {
                task.setReasonForIncompletion(result.getFailureReason());
            }
        } catch (Exception e) {
            logger.error("Task execution failed: {}", task.getReferenceTaskName(), e);
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion(e.getMessage());
        }

        task.setEndTime(Workflow.currentTimeMillis());
        taskOutputs.put(task.getReferenceTaskName(),
                task.getOutputData() != null ? task.getOutputData() : Collections.emptyMap());
    }

    private void updateTaskInWorkflow(TaskModel task) {
        List<TaskModel> workflowTasks = workflowModel.getTasks();
        for (int i = 0; i < workflowTasks.size(); i++) {
            if (workflowTasks.get(i).getTaskId().equals(task.getTaskId())) {
                workflowTasks.set(i, task);
                return;
            }
        }
    }

    private void processSignals() {
        for (Map.Entry<String, Map<String, Object>> entry : pendingSignals.entrySet()) {
            String taskRefName = entry.getKey();
            Map<String, Object> output = entry.getValue();

            Optional<TaskModel> taskOpt = workflowModel.getTasks().stream()
                    .filter(t -> t.getReferenceTaskName().equals(taskRefName))
                    .findFirst();

            if (taskOpt.isPresent()) {
                TaskModel task = taskOpt.get();
                if (task.getStatus() == TaskModel.Status.IN_PROGRESS) {
                    task.setOutputData(output);
                    task.setStatus(TaskModel.Status.COMPLETED);
                    task.setEndTime(Workflow.currentTimeMillis());
                    taskOutputs.put(taskRefName, output);
                }
            }
        }
        pendingSignals.clear();
    }

    // ==================== Dynamic Handler Registration ====================

    /**
     * Register dynamic signal handlers for workflow control operations.
     */
    private void registerSignalHandlers() {
        Workflow.registerListener((DynamicSignalHandler) (signalName, encodedArgs) -> {
            switch (signalName) {
                case "completeTask":
                    String taskRefName = encodedArgs.get(0, String.class);
                    @SuppressWarnings("unchecked")
                    Map<String, Object> output = encodedArgs.get(1, Map.class);
                    completeTask(taskRefName, output);
                    break;
                case "pause":
                    pause();
                    break;
                case "resume":
                    resume();
                    break;
                case "retryFailedTask":
                    String retryTaskRef = encodedArgs.get(0, String.class);
                    retryFailedTask(retryTaskRef);
                    break;
                default:
                    logger.warn("Unknown signal: {}", signalName);
            }
        });
    }

    /**
     * Register dynamic query handlers for workflow state queries.
     */
    private void registerQueryHandlers() {
        Workflow.registerListener((DynamicQueryHandler) (queryType, encodedArgs) -> {
            switch (queryType) {
                case "getWorkflow":
                    return getWorkflow();
                case "getTasks":
                    return getTasks();
                case "getVariables":
                    return getVariables();
                default:
                    logger.warn("Unknown query: {}", queryType);
                    return null;
            }
        });
    }

    // ==================== Signal Method Implementations ====================

    private void completeTask(String taskRefName, Map<String, Object> output) {
        logger.info("Received signal to complete task: {}", taskRefName);
        pendingSignals.put(taskRefName, output);
    }

    private void pause() {
        if (workflowModel.getStatus() == WorkflowModel.Status.RUNNING) {
            isPaused = true;
            workflowModel.setStatus(WorkflowModel.Status.PAUSED);
            updateStatusAttribute(WorkflowModel.Status.PAUSED);
            logger.info("Workflow paused: {}", workflowModel.getWorkflowId());
        }
    }

    private void resume() {
        if (isPaused) {
            isPaused = false;
            workflowModel.setStatus(WorkflowModel.Status.RUNNING);
            updateStatusAttribute(WorkflowModel.Status.RUNNING);
            logger.info("Workflow resumed: {}", workflowModel.getWorkflowId());
        }
    }

    private void retryFailedTask(String taskRefName) {
        Optional<TaskModel> failedTaskOpt = workflowModel.getTasks().stream()
                .filter(t -> t.getReferenceTaskName().equals(taskRefName))
                .filter(t -> t.getStatus() == TaskModel.Status.FAILED
                        || t.getStatus() == TaskModel.Status.FAILED_WITH_TERMINAL_ERROR)
                .findFirst();

        failedTaskOpt.ifPresent(task -> {
            task.setStatus(TaskModel.Status.SCHEDULED);
            task.setRetryCount(task.getRetryCount() + 1);
            task.setReasonForIncompletion(null);
            task.setStartTime(0);
            task.setEndTime(0);
            logger.info("Task {} scheduled for retry (attempt {})",
                    taskRefName, task.getRetryCount());
        });
    }

    // ==================== Query Method Implementations ====================

    private WorkflowState getWorkflow() {
        return WorkflowState.builder()
                .workflowId(workflowModel.getWorkflowId())
                .workflowType(workflowModel.getWorkflowName())
                .version(workflowModel.getWorkflowVersion())
                .status(workflowModel.getStatus().name())
                .reasonForIncompletion(workflowModel.getReasonForIncompletion())
                .createTime(workflowModel.getCreateTime())
                .startTime(workflowModel.getCreateTime())
                .updateTime(lastUpdateTime)
                .endTime(workflowModel.getStatus().isTerminal()
                        ? workflowModel.getEndTime() : null)
                .input(workflowModel.getInput())
                .output(workflowModel.getOutput())
                .variables(workflowModel.getVariables())
                .correlationId(workflowModel.getCorrelationId())
                .priority(workflowModel.getPriority())
                .ownerApp(workflowModel.getOwnerApp())
                .createdBy(workflowModel.getCreatedBy())
                .tasks(workflowModel.getTasks().stream()
                        .map(this::toTaskState)
                        .collect(Collectors.toList()))
                .build();
    }

    private List<TaskState> getTasks() {
        return workflowModel.getTasks().stream()
                .map(this::toTaskState)
                .collect(Collectors.toList());
    }

    private Map<String, Object> getVariables() {
        return workflowModel.getVariables() != null
                ? workflowModel.getVariables()
                : Collections.emptyMap();
    }

    private TaskState toTaskState(TaskModel task) {
        return TaskState.builder()
                .taskId(task.getTaskId())
                .taskType(task.getTaskType())
                .taskDefName(task.getTaskDefName())
                .referenceTaskName(task.getReferenceTaskName())
                .status(task.getStatus().name())
                .reasonForIncompletion(task.getReasonForIncompletion())
                .retryCount(task.getRetryCount())
                .scheduledTime(task.getScheduledTime())
                .startTime(task.getStartTime())
                .updateTime(task.getUpdateTime())
                .endTime(task.getEndTime())
                .inputData(task.getInputData())
                .outputData(task.getOutputData())
                .workerId(task.getWorkerId())
                .pollCount(task.getPollCount())
                .iteration(task.getIteration())
                .build();
    }

    /**
     * Populate TaskDefinition on each WorkflowTask.
     * Required because SimpleTaskMapper expects taskDefinition to be set.
     */
    private void populateTaskDefinitions(WorkflowDef workflowDef, Map<String, String> taskDefsJson) {
        Map<String, TaskDef> taskDefs = new HashMap<>();
        for (Map.Entry<String, String> entry : taskDefsJson.entrySet()) {
            try {
                TaskDef taskDef = objectMapper.readValue(entry.getValue(), TaskDef.class);
                taskDefs.put(entry.getKey(), taskDef);
            } catch (JsonProcessingException e) {
                logger.warn("Failed to parse task definition: {}", entry.getKey());
            }
        }

        populateTaskDefinitionsRecursive(workflowDef.getTasks(), taskDefs);
    }

    private void populateTaskDefinitionsRecursive(List<WorkflowTask> tasks,
                                                   Map<String, TaskDef> taskDefs) {
        if (tasks == null) {
            return;
        }

        for (WorkflowTask task : tasks) {
            if (task.getTaskDefinition() == null && taskDefs.containsKey(task.getName())) {
                task.setTaskDefinition(taskDefs.get(task.getName()));
            }

            // Recurse into nested structures
            if (task.getDecisionCases() != null) {
                for (List<WorkflowTask> caseTasks : task.getDecisionCases().values()) {
                    populateTaskDefinitionsRecursive(caseTasks, taskDefs);
                }
            }
            populateTaskDefinitionsRecursive(task.getDefaultCase(), taskDefs);

            if (task.getForkTasks() != null) {
                for (List<WorkflowTask> forkBranch : task.getForkTasks()) {
                    populateTaskDefinitionsRecursive(forkBranch, taskDefs);
                }
            }

            populateTaskDefinitionsRecursive(task.getLoopOver(), taskDefs);
        }
    }

    /**
     * Disable Conductor timeouts to use Temporal timeouts instead.
     */
    private void disableConductorTimeouts(WorkflowDef workflowDef) {
        workflowDef.setTimeoutSeconds(0);
        disableTaskTimeoutsRecursive(workflowDef.getTasks());
    }

    private void disableTaskTimeoutsRecursive(List<WorkflowTask> tasks) {
        if (tasks == null) {
            return;
        }

        for (WorkflowTask task : tasks) {
            TaskDef taskDef = task.getTaskDefinition();
            if (taskDef != null) {
                taskDef.setTimeoutSeconds(0);
                taskDef.setPollTimeoutSeconds(0);
                taskDef.setResponseTimeoutSeconds(0);
            }

            // Recurse into nested structures
            if (task.getDecisionCases() != null) {
                for (List<WorkflowTask> caseTasks : task.getDecisionCases().values()) {
                    disableTaskTimeoutsRecursive(caseTasks);
                }
            }
            disableTaskTimeoutsRecursive(task.getDefaultCase());

            if (task.getForkTasks() != null) {
                for (List<WorkflowTask> forkBranch : task.getForkTasks()) {
                    disableTaskTimeoutsRecursive(forkBranch);
                }
            }

            disableTaskTimeoutsRecursive(task.getLoopOver());
        }
    }
}
