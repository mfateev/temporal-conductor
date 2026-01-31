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
import io.temporal.workflow.ActivityStub;
import io.temporal.conductor.executor.DeterministicIdGenerator;
import io.temporal.conductor.executor.InMemoryMetadataDAO;
import io.temporal.conductor.executor.SystemTaskExecutor;
import io.temporal.conductor.executor.TemporalDeciderServiceFactory;
import io.temporal.common.converter.EncodedValues;
import io.temporal.conductor.workflow.model.ConductorWorkflowInput;
import io.temporal.conductor.workflow.model.ConductorWorkflowOutput;
import io.temporal.conductor.workflow.model.ContinueAsNewCheckpoint;
import io.temporal.conductor.workflow.model.TaskSnapshot;
import io.temporal.conductor.workflow.model.PendingTimer;
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
    private static final int MAX_ITERATIONS = 10000;

    // Query response truncation settings - prevents timeouts when returning large data
    private static final int MAX_DATA_VALUE_SIZE = 1024; // 1KB per string value
    private static final String TRUNCATION_MARKER = "...[TRUNCATED]";

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Map<String, Promise<TaskExecutionResult>> activityPromises = new HashMap<>();
    private final Set<String> pendingTaskIds = new HashSet<>();
    private final Map<String, Promise<Void>> waitTimers = new HashMap<>();

    private WorkflowModel workflowModel;
    private DeciderService deciderService;
    private InMemoryMetadataDAO metadataDao;
    private SystemTaskExecutor systemTaskExecutor;

    private final Map<String, Map<String, Object>> pendingSignals = new HashMap<>();
    private volatile boolean isPaused = false;
    private long lastUpdateTime = 0;

    private ConductorWorkflowInput workflowInput;
    private DeterministicIdGenerator idGenerator;
    private int continueAsNewCount = 0;

    private final ActivityStub activities = Workflow.newUntypedActivityStub(
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
        ConductorWorkflowInput input = args.get(0, ConductorWorkflowInput.class);
        this.workflowInput = input;

        String workflowRunId = Workflow.getInfo().getRunId();
        String workflowId = Workflow.getInfo().getWorkflowId();
        String workflowType = Workflow.getInfo().getWorkflowType();

        boolean isContinuation = input.getCheckpoint() != null;
        if (isContinuation) {
            continueAsNewCount = input.getCheckpoint().getContinueAsNewCount();
            logger.info("Continuing Conductor workflow execution (continuation #{}), type: {}, runId: {}",
                    continueAsNewCount, workflowType, workflowRunId);
        } else {
            logger.info("Starting Conductor workflow execution, type: {}, runId: {}", workflowType, workflowRunId);
        }

        registerSignalHandlers();
        registerQueryHandlers();
        initializeConductorComponents(input, workflowRunId, workflowId);

        if (isContinuation) {
            restoreFromCheckpoint(input.getCheckpoint());
        }

        WorkflowDef workflowDef = workflowModel.getWorkflowDefinition();
        initializeSearchAttributes(workflowDef, input);

        try {
            runSchedulingLoop();
        } catch (TerminateWorkflowException e) {
            handleTerminateException(e);
        } catch (Exception e) {
            logger.error("Workflow execution failed", e);
            workflowModel.setStatus(WorkflowModel.Status.FAILED);
            workflowModel.setReasonForIncompletion(e.getMessage());
        }

        updateStatusAttribute(workflowModel.getStatus());
        updateFailedTaskNamesAttribute();

        // Don't include taskOutputs in workflow result - it can grow very large with many tasks
        // and cause the WorkflowExecutionCompleted event to exceed the 4MB gRPC limit.
        // Task outputs are still available via the getTasks query or Temporal history.
        return new ConductorWorkflowOutput(
                workflowModel.getStatus().name(),
                workflowModel.getOutput() != null ? workflowModel.getOutput() : Collections.emptyMap(),
                workflowModel.getReasonForIncompletion(),
                Collections.emptyMap()
        );
    }

    private void initializeConductorComponents(
            ConductorWorkflowInput input,
            String workflowRunId,
            String workflowId) {

        metadataDao = new InMemoryMetadataDAO();
        for (Map.Entry<String, String> entry : input.getTaskDefsJson().entrySet()) {
            try {
                TaskDef taskDef = objectMapper.readValue(entry.getValue(), TaskDef.class);
                metadataDao.createTaskDef(taskDef);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to parse task definition: " + entry.getKey(), e);
            }
        }

        long startSequence = input.getCheckpoint() != null
                ? input.getCheckpoint().getLastTaskIdSequence() : 0;
        idGenerator = new DeterministicIdGenerator(workflowRunId, startSequence);
        TemporalDeciderServiceFactory factory =
                new TemporalDeciderServiceFactory(metadataDao, idGenerator);
        deciderService = factory.create();
        systemTaskExecutor = new SystemTaskExecutor(deciderService, objectMapper);

        WorkflowDef workflowDef;
        try {
            workflowDef = objectMapper.readValue(input.getWorkflowDefJson(), WorkflowDef.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse workflow definition", e);
        }
        populateTaskDefinitions(workflowDef, input.getTaskDefsJson());
        disableConductorTimeouts(workflowDef);

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
            if (iterations % 5 == 1) {
                long historySize = Workflow.getInfo().getHistorySize();
                logger.info("Scheduling loop iteration {}, historySize={}KB, taskCount={}",
                        iterations, historySize / 1024, workflowModel.getTasks().size());
            }

            // Check for pause
            if (isPaused) {
                Workflow.await(() -> !isPaused);
                continue;
            }

            // Check for continue-as-new at safe checkpoint (no pending activities)
            if (shouldContinueAsNew()) {
                performContinueAsNew();
                return;  // New workflow run will continue
            }

            DeciderService.DeciderOutcome outcome = deciderService.decide(workflowModel);

            if (DeciderOutcomeAccessor.isComplete(outcome)) {
                completeWorkflow();
                break;
            }

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
                boolean moreWorkNeeded = executeNewlyScheduledTasks();

                // If we hit the iteration limit or continue-as-new is suggested, skip waiting
                // and continue to next main loop iteration
                if (moreWorkNeeded) {
                    logger.debug("executeNewlyScheduledTasks indicates more work needed, continuing main loop");
                    continue;
                }

                // Don't check shouldCompleteWorkflow() here - let DeciderService determine
                // completion on the next iteration. DeciderOutcomeAccessor.isComplete(outcome)
                // at the start of the loop is the authoritative completion check.
                // This ensures tasks after JOIN (like final_task) get scheduled.
                waitForSignalsOrChanges();
                continue;
            }

            for (TaskModel task : tasksToSchedule) {
                task.setWorkflowInstanceId(workflowModel.getWorkflowId());
                workflowModel.getTasks().add(task);
            }

            executeTasks(tasksToSchedule);
            reEvaluateInProgressSystemTasks();
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

        Optional<TaskModel> lastTask = workflowModel.getTasks().stream()
                .filter(t -> t.getStatus() == TaskModel.Status.COMPLETED)
                .reduce((first, second) -> second);

        if (lastTask.isPresent() && (workflowModel.getOutput() == null
                || workflowModel.getOutput().isEmpty())) {
            workflowModel.setOutput(lastTask.get().getOutputData());
        }
    }

    private void waitForSignalsOrChanges() {
        // Check if there are any SCHEDULED tasks that should be executed
        // This can happen if executeNewlyScheduledTasks() hit its maxIterations limit
        boolean hasScheduledTasks = workflowModel.getTasks().stream()
                .anyMatch(t -> t.getStatus() == TaskModel.Status.SCHEDULED);

        // Check if there are IN_PROGRESS system tasks that can progress without external events
        // (e.g., DO_WHILE ready for next iteration, vs WAIT waiting for timer/signal)
        boolean hasProgressableSystemTasks = workflowModel.getTasks().stream()
                .anyMatch(t -> t.getStatus() == TaskModel.Status.IN_PROGRESS
                        && isSystemTask(t)
                        && !TaskType.WAIT.name().equals(t.getTaskType())
                        && !TaskType.JOIN.name().equals(t.getTaskType()));

        // Log detailed task state for debugging
        if (logger.isDebugEnabled()) {
            List<String> inProgressSystemTasks = workflowModel.getTasks().stream()
                    .filter(t -> t.getStatus() == TaskModel.Status.IN_PROGRESS && isSystemTask(t))
                    .map(t -> t.getReferenceTaskName() + "(" + t.getTaskType() + ")")
                    .collect(Collectors.toList());
            logger.debug("waitForSignalsOrChanges: hasScheduledTasks={}, hasProgressableSystemTasks={}, " +
                    "inProgressSystemTasks={}, activityPromises={}, pendingTaskIds={}",
                    hasScheduledTasks, hasProgressableSystemTasks, inProgressSystemTasks,
                    activityPromises.size(), pendingTaskIds.size());
        }

        // If there are scheduled tasks or progressable system tasks, don't wait
        // Let the main loop continue to process them
        if (hasScheduledTasks || hasProgressableSystemTasks) {
            logger.info("Not waiting - hasScheduledTasks: {}, hasProgressableSystemTasks: {}",
                    hasScheduledTasks, hasProgressableSystemTasks);
            return;
        }

        // Check if there are any IN_PROGRESS tasks that might be waiting for external events
        // (e.g., WAIT tasks, JOIN waiting for branches)
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

        processCompletedActivities();

        if (shouldContinueAsNew()) {
            performContinueAsNew();
            return;
        }

        processCompletedWaitTimers();

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
                Optional<TaskModel> taskOpt = workflowModel.getTasks().stream()
                        .filter(t -> t.getReferenceTaskName().equals(taskRefName))
                        .findFirst();

                if (taskOpt.isPresent()) {
                    TaskModel task = taskOpt.get();
                    if (task.getStatus() == TaskModel.Status.IN_PROGRESS) {
                        logger.debug("WAIT timer completed for task: {}", taskRefName);
                        task.setStatus(TaskModel.Status.COMPLETED);
                        task.setEndTime(Workflow.currentTimeMillis());
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
        Set<String> taskIdsToProcess = tasks.stream()
                .map(TaskModel::getTaskId)
                .collect(Collectors.toSet());

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

        List<TaskModel> asyncSystemTasks = new ArrayList<>();
        List<TaskModel> syncSystemTasks = new ArrayList<>();

        for (TaskModel task : systemTasks) {
            if (systemTaskExecutor.isAsync(task.getTaskType())) {
                asyncSystemTasks.add(task);
            } else {
                syncSystemTasks.add(task);
            }
        }

        for (TaskModel task : syncSystemTasks) {
            executeSystemTask(task);
        }

        List<TaskModel> newlyScheduledTasks = workflowModel.getTasks().stream()
                .filter(t -> t.getStatus() == TaskModel.Status.SCHEDULED
                        && !taskIdsToProcess.contains(t.getTaskId()))
                .collect(Collectors.toList());

        List<TaskModel> allWorkerTasks = new ArrayList<>(workerTasks);
        List<TaskModel> newSystemTasks = new ArrayList<>();

        for (TaskModel task : newlyScheduledTasks) {
            if (isSystemTask(task) && !systemTaskExecutor.isAsync(task.getTaskType())) {
                newSystemTasks.add(task);
            } else if (!isSystemTask(task)) {
                allWorkerTasks.add(task);
            }
        }

        for (TaskModel task : newSystemTasks) {
            executeSystemTask(task);
        }

        executeWorkerTasks(allWorkerTasks);

        for (TaskModel task : asyncSystemTasks) {
            executeSystemTask(task);
        }
    }

    private boolean isSystemTask(TaskModel task) {
        return SystemTaskExecutor.isKnownSystemTaskType(task.getTaskType());
    }

    private void executeSystemTask(TaskModel task) {
        logger.debug("Executing system task: {} ({})",
                task.getReferenceTaskName(), task.getTaskType());

        if (task.getStartTime() == 0L) {
            task.setStartTime(Workflow.currentTimeMillis());
        }

        if (TaskType.WAIT.name().equals(task.getTaskType())) {
            executeWaitTask(task);
            return;
        }

        systemTaskExecutor.execute(workflowModel, task);

        if (task.getStatus().isTerminal() && task.getEndTime() == 0L) {
            task.setEndTime(Workflow.currentTimeMillis());
        }

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

        if (task.getStatus() == TaskModel.Status.SCHEDULED) {
            task.setStatus(TaskModel.Status.IN_PROGRESS);
        }

        if (task.getStatus().isTerminal()) {
            if (task.getEndTime() == 0L) {
                task.setEndTime(Workflow.currentTimeMillis());
            }
            return;
        }

        if (waitTimers.containsKey(taskRefName)) {
            Promise<Void> existingTimer = waitTimers.get(taskRefName);
            if (existingTimer.isCompleted()) {
                task.setStatus(TaskModel.Status.COMPLETED);
                task.setEndTime(Workflow.currentTimeMillis());
                waitTimers.remove(taskRefName);
                logger.debug("WAIT task {} completed by timer", taskRefName);
            }
            return;
        }

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
                long waitTimeout = task.getWaitTimeout();
                if (waitTimeout > 0) {
                    long currentTime = Workflow.currentTimeMillis();
                    long delayMs = waitTimeout - currentTime;
                    if (delayMs > 0) {
                        Promise<Void> timer = Workflow.newTimer(Duration.ofMillis(delayMs));
                        waitTimers.put(taskRefName, timer);
                        logger.debug("Started WAIT timer for task {} with delay {}ms (until mode)", taskRefName, delayMs);
                    } else {
                                task.setStatus(TaskModel.Status.COMPLETED);
                        task.setEndTime(Workflow.currentTimeMillis());
                    }
                    return;
                }
            }
        }
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
     * @return true if more work is needed (hit iteration limit or continue-as-new suggested)
     */
    private boolean executeNewlyScheduledTasks() {
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

            // Re-evaluate IN_PROGRESS system tasks (DO_WHILE, JOIN)
            reEvaluateInProgressSystemTasks();

            // Check if continue-as-new is suggested after activity completion.
            // Each activity is a yield point where WorkflowInfo is refreshed.
            // This breaks the tight loop for long-running workflows.
            if (Workflow.getInfo().isContinueAsNewSuggested() && canContinueAsNew()) {
                logger.info("Continue-as-new suggested during executeNewlyScheduledTasks, " +
                        "breaking to main loop");
                return true;  // More work needed - continue-as-new check in main loop
            }
        }

        // Check if we hit the iteration limit with more scheduled tasks remaining
        boolean hasMoreScheduledTasks = workflowModel.getTasks().stream()
                .anyMatch(t -> t.getStatus() == TaskModel.Status.SCHEDULED);

        // Check for IN_PROGRESS system tasks that should trigger more work
        boolean hasProgressableSystemTasks = workflowModel.getTasks().stream()
                .anyMatch(t -> t.getStatus() == TaskModel.Status.IN_PROGRESS
                        && isSystemTask(t)
                        && !TaskType.WAIT.name().equals(t.getTaskType())
                        && !TaskType.JOIN.name().equals(t.getTaskType()));

        if (iterations >= maxIterations && hasMoreScheduledTasks) {
            logger.info("executeNewlyScheduledTasks hit iteration limit ({}) with more tasks pending",
                    maxIterations);
            return true;  // More work needed
        }

        // Also return true if there are progressable system tasks that need re-evaluation
        if (hasProgressableSystemTasks) {
            logger.info("executeNewlyScheduledTasks: returning true due to progressable system tasks " +
                    "(iterations={}, hasMoreScheduledTasks={})", iterations, hasMoreScheduledTasks);
            return true;  // More work needed - system tasks can schedule more work
        }

        logger.debug("executeNewlyScheduledTasks: returning false (iterations={}, hasMoreScheduledTasks={}, " +
                "hasProgressableSystemTasks={})", iterations, hasMoreScheduledTasks, hasProgressableSystemTasks);
        return false;  // All scheduled tasks processed
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

        // Start activity asynchronously - activity type is the task definition name
        String activityType = task.getTaskDefName();
        Promise<TaskExecutionResult> promise = activities.executeAsync(
                activityType,
                TaskExecutionResult.class,
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
            // Activity type is the task definition name
            String activityType = task.getTaskDefName();
            TaskExecutionResult result = activities.execute(
                    activityType,
                    TaskExecutionResult.class,
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
                }
            }
        }
        pendingSignals.clear();
    }

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

    @SuppressWarnings("unchecked")
    private Map<String, Object> truncateData(Map<String, Object> data) {
        if (data == null) {
            return null;
        }
        Map<String, Object> truncated = new HashMap<>();
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof String) {
                String strValue = (String) value;
                if (strValue.length() > MAX_DATA_VALUE_SIZE) {
                    truncated.put(entry.getKey(),
                            strValue.substring(0, MAX_DATA_VALUE_SIZE) + TRUNCATION_MARKER);
                } else {
                    truncated.put(entry.getKey(), value);
                }
            } else if (value instanceof Map) {
                truncated.put(entry.getKey(), truncateData((Map<String, Object>) value));
            } else {
                truncated.put(entry.getKey(), value);
            }
        }
        return truncated;
    }

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
                .input(truncateData(workflowModel.getInput()))
                .output(truncateData(workflowModel.getOutput()))
                .variables(truncateData(workflowModel.getVariables()))
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
                .inputData(truncateData(task.getInputData()))
                .outputData(truncateData(task.getOutputData()))
                .workerId(task.getWorkerId())
                .pollCount(task.getPollCount())
                .iteration(task.getIteration())
                .build();
    }

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

    private boolean canContinueAsNew() {
        return activityPromises.isEmpty()
                && !isPaused
                && !workflowModel.getStatus().isTerminal();
    }

    private boolean shouldContinueAsNew() {
        boolean suggested = Workflow.getInfo().isContinueAsNewSuggested();
        boolean canDo = canContinueAsNew();
        int taskCount = workflowModel.getTasks().size();
        long historyLength = Workflow.getInfo().getHistoryLength();
        long historySize = Workflow.getInfo().getHistorySize();

        // Trigger continue-as-new based on history LENGTH in addition to size.
        // With large payloads, replay time becomes an issue before the size threshold (~40MB).
        // Workflow task timeout (default 10s) can be exceeded when replaying many events
        // with large payloads. Threshold of 500 events is roughly 80 activities (~6 events each).
        boolean historyLengthThreshold = historyLength > 500;

        // Log every 5 tasks or when thresholds are approached
        if (suggested || historyLengthThreshold || taskCount % 5 == 0) {
            logger.info("Continue-as-new check: suggested={}, historyLengthThreshold={}, " +
                    "canContinueAsNew={}, historySize={}KB, historyLength={}, taskCount={}",
                    suggested, historyLengthThreshold, canDo, historySize / 1024, historyLength, taskCount);
        }

        return (suggested || historyLengthThreshold) && canDo;
    }

    private void performContinueAsNew() {
        logger.info("Performing continue-as-new (history length: {}, continuation #{})",
                Workflow.getInfo().getHistoryLength(), continueAsNewCount + 1);

        // Capture checkpoint state
        ContinueAsNewCheckpoint checkpoint = new ContinueAsNewCheckpoint();
        checkpoint.setCompletedTasks(tasksToSnapshots(workflowModel.getTasks()));
        checkpoint.setVariables(workflowModel.getVariables() != null
                ? new HashMap<>(workflowModel.getVariables()) : new HashMap<>());
        checkpoint.setCorrelationId(workflowModel.getCorrelationId());
        checkpoint.setPriority(workflowModel.getPriority());
        checkpoint.setOwnerApp(workflowModel.getOwnerApp());
        checkpoint.setCreatedBy(workflowModel.getCreatedBy());
        checkpoint.setOriginalCreateTime(workflowModel.getCreateTime());
        checkpoint.setLastTaskIdSequence(idGenerator.getCurrentSequence());
        checkpoint.setPendingTimers(capturePendingTimers());
        checkpoint.setPendingSignals(new HashMap<>(pendingSignals));
        checkpoint.setContinueAsNewCount(continueAsNewCount + 1);

        ConductorWorkflowInput newInput = ConductorWorkflowInput.builder()
                .workflowDefJson(workflowInput.getWorkflowDefJson())
                .workflowInput(workflowInput.getWorkflowInput())
                .taskDefsJson(workflowInput.getTaskDefsJson())
                .correlationId(workflowInput.getCorrelationId())
                .priority(workflowInput.getPriority())
                .ownerApp(workflowInput.getOwnerApp())
                .createdBy(workflowInput.getCreatedBy())
                .tags(workflowInput.getTags())
                .checkpoint(checkpoint)
                .build();

        Workflow.continueAsNew(newInput);
    }

    private void restoreFromCheckpoint(ContinueAsNewCheckpoint checkpoint) {
        logger.info("Restoring from checkpoint (continuation #{}, {} tasks)",
                checkpoint.getContinueAsNewCount(),
                checkpoint.getCompletedTasks().size());

        workflowModel.getTasks().clear();
        workflowModel.getTasks().addAll(snapshotsToTasks(checkpoint.getCompletedTasks()));

        if (checkpoint.getVariables() != null) {
            workflowModel.setVariables(new HashMap<>(checkpoint.getVariables()));
        }

        workflowModel.setCorrelationId(checkpoint.getCorrelationId());
        workflowModel.setPriority(checkpoint.getPriority());
        workflowModel.setOwnerApp(checkpoint.getOwnerApp());
        workflowModel.setCreatedBy(checkpoint.getCreatedBy());
        workflowModel.setCreateTime(checkpoint.getOriginalCreateTime());

        pendingSignals.putAll(checkpoint.getPendingSignals());

        for (PendingTimer timer : checkpoint.getPendingTimers()) {
            if (timer.getRemainingDurationMs() > 0) {
                Promise<Void> newTimer = Workflow.newTimer(Duration.ofMillis(timer.getRemainingDurationMs()));
                waitTimers.put(timer.getTaskRefName(), newTimer);
                logger.debug("Recreated timer for task {} with {}ms remaining",
                        timer.getTaskRefName(), timer.getRemainingDurationMs());
            }
        }
    }

    private List<PendingTimer> capturePendingTimers() {
        List<PendingTimer> timers = new ArrayList<>();
        long currentTime = Workflow.currentTimeMillis();

        for (Map.Entry<String, Promise<Void>> entry : waitTimers.entrySet()) {
            String taskRefName = entry.getKey();

            Optional<TaskModel> taskOpt = workflowModel.getTasks().stream()
                    .filter(t -> t.getReferenceTaskName().equals(taskRefName))
                    .findFirst();

            if (taskOpt.isPresent()) {
                TaskModel task = taskOpt.get();
                long waitTimeout = task.getWaitTimeout();
                if (waitTimeout > 0) {
                    long remaining = waitTimeout - currentTime;
                    if (remaining > 0) {
                        timers.add(new PendingTimer(taskRefName, remaining));
                    }
                }
            }
        }

        return timers;
    }

    private List<TaskSnapshot> tasksToSnapshots(List<TaskModel> tasks) {
        List<TaskSnapshot> snapshots = new ArrayList<>();
        for (TaskModel task : tasks) {
            TaskSnapshot snapshot = new TaskSnapshot();
            snapshot.setTaskId(task.getTaskId());
            snapshot.setTaskRefName(task.getReferenceTaskName());
            snapshot.setTaskType(task.getTaskType());
            snapshot.setTaskDefName(task.getTaskDefName());
            snapshot.setStatus(task.getStatus() != null ? task.getStatus().name() : null);
            snapshot.setInputData(task.getInputData() != null ? new HashMap<>(task.getInputData()) : new HashMap<>());
            snapshot.setOutputData(task.getOutputData() != null ? new HashMap<>(task.getOutputData()) : new HashMap<>());
            snapshot.setRetryCount(task.getRetryCount());
            snapshot.setSeq(task.getSeq());
            snapshot.setIteration(task.getIteration());
            snapshot.setScheduledTime(task.getScheduledTime());
            snapshot.setStartTime(task.getStartTime());
            snapshot.setEndTime(task.getEndTime());
            snapshots.add(snapshot);
        }
        return snapshots;
    }

    private List<TaskModel> snapshotsToTasks(List<TaskSnapshot> snapshots) {
        List<TaskModel> tasks = new ArrayList<>();
        for (TaskSnapshot snapshot : snapshots) {
            TaskModel task = new TaskModel();
            task.setTaskId(snapshot.getTaskId());
            task.setReferenceTaskName(snapshot.getTaskRefName());
            task.setTaskType(snapshot.getTaskType());
            task.setTaskDefName(snapshot.getTaskDefName());
            if (snapshot.getStatus() != null) {
                task.setStatus(TaskModel.Status.valueOf(snapshot.getStatus()));
            }
            task.setInputData(snapshot.getInputData() != null ? new HashMap<>(snapshot.getInputData()) : new HashMap<>());
            task.setOutputData(snapshot.getOutputData() != null ? new HashMap<>(snapshot.getOutputData()) : new HashMap<>());
            task.setRetryCount(snapshot.getRetryCount());
            task.setSeq(snapshot.getSeq());
            task.setIteration(snapshot.getIteration());
            task.setScheduledTime(snapshot.getScheduledTime());
            task.setStartTime(snapshot.getStartTime());
            task.setEndTime(snapshot.getEndTime());
            tasks.add(task);
        }
        return tasks;
    }
}
