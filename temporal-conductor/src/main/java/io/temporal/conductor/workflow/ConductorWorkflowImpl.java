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
import com.netflix.conductor.common.metadata.tasks.TaskDef.RetryLogic;
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
import io.temporal.conductor.activity.EventPublishActivity;
import io.temporal.workflow.ActivityStub;
import io.temporal.api.enums.v1.ParentClosePolicy;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.ChildWorkflowStub;
import io.temporal.conductor.executor.DeterministicIdGenerator;
import io.temporal.conductor.executor.InMemoryMetadataDAO;
import io.temporal.conductor.executor.SystemTaskExecutor;
import io.temporal.conductor.executor.TemporalDeciderServiceFactory;
import io.temporal.conductor.handler.TaskExecutionContext;
import io.temporal.conductor.handler.TaskTypeHandler;
import io.temporal.conductor.handler.TaskTypeHandlerFactory;
import io.temporal.conductor.handler.TaskTypeHandlerRegistry;
import io.temporal.conductor.handler.impl.TaskExecutionContextImpl;
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

    // Event-driven state tracking - Promise.handle callbacks set this flag
    private volatile boolean stateUpdated = false;
    private final Set<String> pendingActivityTaskIds = new HashSet<>();
    private final Set<String> pendingTimerTaskRefs = new HashSet<>();
    private final Set<String> pendingChildWorkflowTaskIds = new HashSet<>();

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

    // Task type handler infrastructure
    private TaskTypeHandlerRegistry handlerRegistry;
    private TaskExecutionContext executionContext;

    // Default timeouts when TaskDef doesn't specify values
    private static final Duration DEFAULT_START_TO_CLOSE_TIMEOUT = Duration.ofMinutes(10);
    private static final Duration DEFAULT_SCHEDULE_TO_START_TIMEOUT = Duration.ofMinutes(5);
    private static final int DEFAULT_RETRY_COUNT = 3;
    private static final Duration DEFAULT_RETRY_DELAY = Duration.ofSeconds(1);

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
                // Disable Conductor-level timeouts since Temporal handles timeouts
                taskDef.setTimeoutSeconds(0);
                taskDef.setPollTimeoutSeconds(0);
                taskDef.setResponseTimeoutSeconds(0);
                metadataDao.createTaskDef(taskDef);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to parse task definition: " + entry.getKey(), e);
            }
        }

        // Register sub-workflow definitions for SUB_WORKFLOW tasks
        if (input.getWorkflowDefsJson() != null) {
            for (Map.Entry<String, String> entry : input.getWorkflowDefsJson().entrySet()) {
                try {
                    WorkflowDef subWorkflowDef = objectMapper.readValue(entry.getValue(), WorkflowDef.class);
                    metadataDao.createWorkflowDef(subWorkflowDef);
                    logger.debug("Registered sub-workflow definition: {} version {}",
                            subWorkflowDef.getName(), subWorkflowDef.getVersion());
                } catch (JsonProcessingException e) {
                    logger.warn("Failed to parse sub-workflow definition: {}", entry.getKey(), e);
                }
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

        // Initialize task type handler infrastructure
        handlerRegistry = TaskTypeHandlerFactory.createRegistry();
        executionContext = new TaskExecutionContextImpl(
                objectMapper,
                deciderService,
                systemTaskExecutor,
                metadataDao,
                input,
                pendingActivityTaskIds,
                pendingTimerTaskRefs,
                pendingChildWorkflowTaskIds,
                pendingSignals,
                updated -> stateUpdated = updated
        );
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
                        && !TaskType.HUMAN.name().equals(t.getTaskType())
                        && !TaskType.JOIN.name().equals(t.getTaskType())
                        && !TaskType.SUB_WORKFLOW.name().equals(t.getTaskType()));

        // Log detailed task state for debugging
        if (logger.isDebugEnabled()) {
            List<String> inProgressSystemTasks = workflowModel.getTasks().stream()
                    .filter(t -> t.getStatus() == TaskModel.Status.IN_PROGRESS && isSystemTask(t))
                    .map(t -> t.getReferenceTaskName() + "(" + t.getTaskType() + ")")
                    .collect(Collectors.toList());
            logger.debug("waitForSignalsOrChanges: hasScheduledTasks={}, hasProgressableSystemTasks={}, " +
                    "inProgressSystemTasks={}, pendingActivities={}, pendingTimers={}, pendingChildWorkflows={}",
                    hasScheduledTasks, hasProgressableSystemTasks, inProgressSystemTasks,
                    pendingActivityTaskIds.size(), pendingTimerTaskRefs.size(), pendingChildWorkflowTaskIds.size());
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

        // If there are no pending activities, timers, child workflows, signals, AND no IN_PROGRESS tasks,
        // don't wait - just return and let the scheduling loop continue
        if (pendingActivityTaskIds.isEmpty() && pendingTimerTaskRefs.isEmpty()
                && pendingChildWorkflowTaskIds.isEmpty()
                && pendingSignals.isEmpty() && !hasInProgressWaitingTasks) {
            logger.debug("No pending work to wait for, continuing to next iteration");
            return;
        }

        logger.debug("Waiting for events... (activities: {}, timers: {}, childWorkflows: {}, signals: {}, inProgress: {})",
                pendingActivityTaskIds.size(), pendingTimerTaskRefs.size(),
                pendingChildWorkflowTaskIds.size(), pendingSignals.size(), hasInProgressWaitingTasks);

        // Reset state flag before waiting
        stateUpdated = false;

        // Wait for ANY event: activity completion (via callback), signal, or timer completion
        // Promise.handle callbacks set stateUpdated=true when activities/timers complete
        Workflow.await(() -> stateUpdated || !pendingSignals.isEmpty());

        // Reset for next wait cycle
        stateUpdated = false;

        if (shouldContinueAsNew()) {
            performContinueAsNew();
            return;
        }

        if (!pendingSignals.isEmpty()) {
            processSignals();
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

        // Dispatch to the appropriate handler via the registry
        TaskTypeHandler handler = handlerRegistry.getHandler(task.getTaskType());
        handler.execute(task, workflowModel, executionContext);

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

        // If timer already started, nothing more to do - callback will complete it
        if (pendingTimerTaskRefs.contains(taskRefName)) {
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
                    startWaitTimer(taskRefName, duration);
                    return;
                }
            } else if (untilStr != null && !untilStr.isEmpty()) {
                long waitTimeout = task.getWaitTimeout();
                if (waitTimeout > 0) {
                    long currentTime = Workflow.currentTimeMillis();
                    long delayMs = waitTimeout - currentTime;
                    if (delayMs > 0) {
                        startWaitTimer(taskRefName, Duration.ofMillis(delayMs));
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
     * Execute a HUMAN task.
     * HUMAN tasks wait indefinitely for external completion via signal.
     * They are completed by calling the completeTask signal with the task reference name.
     */
    private void executeHumanTask(TaskModel task) {
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

        // HUMAN tasks wait indefinitely for a completeTask signal
        logger.info("HUMAN task {} waiting for manual completion via signal", taskRefName);
    }

    /**
     * Start a WAIT timer with callback-based completion.
     */
    private void startWaitTimer(String taskRefName, Duration duration) {
        pendingTimerTaskRefs.add(taskRefName);
        Promise<Void> timer = Workflow.newTimer(duration);
        logger.debug("Started WAIT timer for task {} with duration {}", taskRefName, duration);

        // Event-driven completion via Promise.handle callback
        timer.handle((result, failure) -> {
            handleTimerCompletion(taskRefName);
            return null;
        });
    }

    /**
     * Handle timer completion - called by Promise.handle callback.
     * Completes the WAIT task and sets stateUpdated flag.
     */
    private void handleTimerCompletion(String taskRefName) {
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

        pendingTimerTaskRefs.remove(taskRefName);
        stateUpdated = true;  // Signal main loop to wake up
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
     * Execute SUB_WORKFLOW task by starting a child workflow.
     * The child workflow executes the sub-workflow definition and its completion
     * is handled asynchronously via Promise.handle callback.
     */
    @SuppressWarnings("unchecked")
    private void executeSubWorkflowTask(TaskModel task) {
        String taskRefName = task.getReferenceTaskName();
        String taskId = task.getTaskId();

        if (task.getStatus() == TaskModel.Status.SCHEDULED) {
            task.setStatus(TaskModel.Status.IN_PROGRESS);
            task.setStartTime(Workflow.currentTimeMillis());
        }

        // If child workflow already started, nothing more to do - callback will complete it
        if (pendingChildWorkflowTaskIds.contains(taskId)) {
            return;
        }

        Map<String, Object> inputData = task.getInputData();
        if (inputData == null) {
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion("SUB_WORKFLOW task has no input data");
            task.setEndTime(Workflow.currentTimeMillis());
            return;
        }

        // Extract sub-workflow parameters from task input
        String subWorkflowName = (String) inputData.get("subWorkflowName");
        Integer subWorkflowVersion = inputData.get("subWorkflowVersion") != null
                ? ((Number) inputData.get("subWorkflowVersion")).intValue() : null;
        Object subWorkflowDefinitionObj = inputData.get("subWorkflowDefinition");
        @SuppressWarnings("unchecked")
        Map<String, Object> subWorkflowInput = (Map<String, Object>) inputData.get("workflowInput");

        if (subWorkflowInput == null) {
            subWorkflowInput = Collections.emptyMap();
        }

        // Resolve the sub-workflow definition
        WorkflowDef subWorkflowDef = null;
        if (subWorkflowDefinitionObj != null) {
            // Inline workflow definition provided - can be WorkflowDef or Map
            if (subWorkflowDefinitionObj instanceof WorkflowDef) {
                subWorkflowDef = (WorkflowDef) subWorkflowDefinitionObj;
            } else if (subWorkflowDefinitionObj instanceof Map) {
                try {
                    String defJson = objectMapper.writeValueAsString(subWorkflowDefinitionObj);
                    subWorkflowDef = objectMapper.readValue(defJson, WorkflowDef.class);
                } catch (JsonProcessingException e) {
                    task.setStatus(TaskModel.Status.FAILED);
                    task.setReasonForIncompletion("Failed to parse inline subWorkflowDefinition: " + e.getMessage());
                    task.setEndTime(Workflow.currentTimeMillis());
                    return;
                }
            } else {
                task.setStatus(TaskModel.Status.FAILED);
                task.setReasonForIncompletion("Invalid subWorkflowDefinition type: " + subWorkflowDefinitionObj.getClass().getName());
                task.setEndTime(Workflow.currentTimeMillis());
                return;
            }
        } else if (subWorkflowName != null) {
            // Look up workflow definition from metadata
            Optional<WorkflowDef> defOpt = subWorkflowVersion != null
                    ? metadataDao.getWorkflowDef(subWorkflowName, subWorkflowVersion)
                    : metadataDao.getLatestWorkflowDef(subWorkflowName);

            if (defOpt.isEmpty()) {
                task.setStatus(TaskModel.Status.FAILED);
                task.setReasonForIncompletion("Sub-workflow definition not found: " + subWorkflowName
                        + (subWorkflowVersion != null ? " version " + subWorkflowVersion : ""));
                task.setEndTime(Workflow.currentTimeMillis());
                return;
            }
            subWorkflowDef = defOpt.get();
        } else {
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion("SUB_WORKFLOW task requires either subWorkflowName or subWorkflowDefinition");
            task.setEndTime(Workflow.currentTimeMillis());
            return;
        }

        // Build child workflow input
        String subWorkflowDefJson;
        try {
            subWorkflowDefJson = objectMapper.writeValueAsString(subWorkflowDef);
        } catch (JsonProcessingException e) {
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion("Failed to serialize sub-workflow definition: " + e.getMessage());
            task.setEndTime(Workflow.currentTimeMillis());
            return;
        }

        ConductorWorkflowInput childInput = ConductorWorkflowInput.builder()
                .workflowDefJson(subWorkflowDefJson)
                .workflowInput(subWorkflowInput)
                .taskDefsJson(workflowInput.getTaskDefsJson())
                .workflowDefsJson(workflowInput.getWorkflowDefsJson())
                .correlationId(workflowInput.getCorrelationId())
                .build();

        // Generate child workflow ID: parentWorkflowId-taskRefName
        String childWorkflowId = Workflow.getInfo().getWorkflowId() + "-" + taskRefName;

        // Create child workflow options
        ChildWorkflowOptions options = ChildWorkflowOptions.newBuilder()
                .setWorkflowId(childWorkflowId)
                .setTaskQueue(Workflow.getInfo().getTaskQueue())
                .build();

        // Start child workflow asynchronously
        // Use the Conductor workflow name as the Temporal workflow type (same pattern as parent workflow)
        String childWorkflowType = subWorkflowDef.getName();
        ChildWorkflowStub childStub = Workflow.newUntypedChildWorkflowStub(childWorkflowType, options);
        Promise<ConductorWorkflowOutput> promise = childStub.executeAsync(
                ConductorWorkflowOutput.class, childInput);

        pendingChildWorkflowTaskIds.add(taskId);
        logger.info("Started child workflow {} for SUB_WORKFLOW task {}", childWorkflowId, taskRefName);

        // Event-driven completion via Promise.handle callback
        promise.handle((result, failure) -> {
            handleChildWorkflowCompletion(taskId, result, failure);
            return null;
        });
    }

    /**
     * Handle child workflow completion - called by Promise.handle callback.
     * Updates the SUB_WORKFLOW task state and sets stateUpdated flag to wake up main loop.
     */
    private void handleChildWorkflowCompletion(String taskId, ConductorWorkflowOutput result, Throwable failure) {
        TaskModel task = getTaskById(taskId);
        if (task == null) {
            logger.warn("Child workflow completed but task not found: {}", taskId);
            pendingChildWorkflowTaskIds.remove(taskId);
            stateUpdated = true;
            return;
        }

        if (failure != null) {
            logger.debug("Child workflow failed for task {}: {}",
                    task.getReferenceTaskName(), failure.getMessage());
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion(failure.getMessage());
        } else if (result != null) {
            task.setOutputData(result.getOutput() != null ? result.getOutput() : Collections.emptyMap());
            if ("COMPLETED".equals(result.getStatus())) {
                task.setStatus(TaskModel.Status.COMPLETED);
                logger.debug("Child workflow completed successfully for task {}",
                        task.getReferenceTaskName());
            } else {
                task.setStatus(TaskModel.Status.FAILED);
                task.setReasonForIncompletion(result.getFailureReason() != null
                        ? result.getFailureReason() : "Child workflow failed with status: " + result.getStatus());
                logger.debug("Child workflow failed for task {}: status={}",
                        task.getReferenceTaskName(), result.getStatus());
            }
        } else {
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion("Child workflow returned null result");
        }

        task.setEndTime(Workflow.currentTimeMillis());
        pendingChildWorkflowTaskIds.remove(taskId);
        stateUpdated = true;  // Signal main loop to wake up
    }

    /**
     * Execute START_WORKFLOW task - fire-and-forget child workflow.
     * Unlike SUB_WORKFLOW, this task completes immediately after starting the child workflow.
     * The child workflow continues independently even if the parent completes or fails.
     */
    @SuppressWarnings("unchecked")
    private void executeStartWorkflowTask(TaskModel task) {
        String taskRefName = task.getReferenceTaskName();

        if (task.getStatus() == TaskModel.Status.SCHEDULED) {
            task.setStatus(TaskModel.Status.IN_PROGRESS);
            task.setStartTime(Workflow.currentTimeMillis());
        }

        // START_WORKFLOW completes immediately - no tracking of child workflow
        if (task.getStatus().isTerminal()) {
            return;
        }

        Map<String, Object> inputData = task.getInputData();
        if (inputData == null) {
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion("START_WORKFLOW task has no input data");
            task.setEndTime(Workflow.currentTimeMillis());
            return;
        }

        // Extract start workflow parameters from task input
        // START_WORKFLOW uses "startWorkflow" nested object for configuration
        @SuppressWarnings("unchecked")
        Map<String, Object> startWorkflowConfig = (Map<String, Object>) inputData.get("startWorkflow");
        if (startWorkflowConfig == null) {
            // Fall back to direct parameters (alternative input format)
            startWorkflowConfig = inputData;
        }

        String workflowName = (String) startWorkflowConfig.get("name");
        Integer workflowVersion = startWorkflowConfig.get("version") != null
                ? ((Number) startWorkflowConfig.get("version")).intValue() : null;
        @SuppressWarnings("unchecked")
        Map<String, Object> childWorkflowInput = (Map<String, Object>) startWorkflowConfig.get("input");
        String correlationId = (String) startWorkflowConfig.get("correlationId");

        if (workflowName == null) {
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion("START_WORKFLOW task requires 'name' parameter");
            task.setEndTime(Workflow.currentTimeMillis());
            return;
        }

        if (childWorkflowInput == null) {
            childWorkflowInput = Collections.emptyMap();
        }

        // Look up workflow definition from metadata
        Optional<WorkflowDef> defOpt = workflowVersion != null
                ? metadataDao.getWorkflowDef(workflowName, workflowVersion)
                : metadataDao.getLatestWorkflowDef(workflowName);

        if (defOpt.isEmpty()) {
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion("Workflow definition not found: " + workflowName
                    + (workflowVersion != null ? " version " + workflowVersion : ""));
            task.setEndTime(Workflow.currentTimeMillis());
            return;
        }

        WorkflowDef childWorkflowDef = defOpt.get();

        // Build child workflow input
        String childWorkflowDefJson;
        try {
            childWorkflowDefJson = objectMapper.writeValueAsString(childWorkflowDef);
        } catch (JsonProcessingException e) {
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion("Failed to serialize workflow definition: " + e.getMessage());
            task.setEndTime(Workflow.currentTimeMillis());
            return;
        }

        ConductorWorkflowInput childInput = ConductorWorkflowInput.builder()
                .workflowDefJson(childWorkflowDefJson)
                .workflowInput(childWorkflowInput)
                .taskDefsJson(workflowInput.getTaskDefsJson())
                .workflowDefsJson(workflowInput.getWorkflowDefsJson())
                .correlationId(correlationId != null ? correlationId : workflowInput.getCorrelationId())
                .build();

        // Generate child workflow ID: parentWorkflowId-taskRefName
        String childWorkflowId = Workflow.getInfo().getWorkflowId() + "-" + taskRefName;

        // Create child workflow options with ABANDON policy - fire-and-forget
        // The child workflow will continue running even if parent completes or fails
        ChildWorkflowOptions options = ChildWorkflowOptions.newBuilder()
                .setWorkflowId(childWorkflowId)
                .setTaskQueue(Workflow.getInfo().getTaskQueue())
                .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON)
                .build();

        // Start child workflow - fire and forget
        String childWorkflowType = childWorkflowDef.getName();
        ChildWorkflowStub childStub = Workflow.newUntypedChildWorkflowStub(childWorkflowType, options);

        // Start the child workflow asynchronously but don't wait for result
        // executeAsync returns a Promise but we intentionally don't track it
        // since this is fire-and-forget (ParentClosePolicy.ABANDON ensures child continues)
        childStub.executeAsync(ConductorWorkflowOutput.class, childInput);

        logger.info("Started fire-and-forget child workflow {} for START_WORKFLOW task {}",
                childWorkflowId, taskRefName);

        // Complete the task immediately with the child workflow ID as output
        Map<String, Object> outputData = new HashMap<>();
        outputData.put("workflowId", childWorkflowId);
        outputData.put("workflowType", childWorkflowType);
        task.setOutputData(outputData);
        task.setStatus(TaskModel.Status.COMPLETED);
        task.setEndTime(Workflow.currentTimeMillis());
    }

    /**
     * Execute EVENT task - publishes an event to a queue via activity.
     * Reuses Conductor's Event task logic for payload preparation and queue name computation.
     */
    private void executeEventTask(TaskModel task) {
        String taskRefName = task.getReferenceTaskName();

        if (task.getStatus() == TaskModel.Status.SCHEDULED) {
            task.setStatus(TaskModel.Status.IN_PROGRESS);
            task.setStartTime(Workflow.currentTimeMillis());
        }

        if (task.getStatus().isTerminal()) {
            return;
        }

        // Prepare event payload (reusing Conductor's Event task logic)
        Map<String, Object> payload = new HashMap<>(
                task.getInputData() != null ? task.getInputData() : Collections.emptyMap());
        payload.put("workflowInstanceId", workflowModel.getWorkflowId());
        payload.put("workflowType", workflowModel.getWorkflowName());
        payload.put("workflowVersion", workflowModel.getWorkflowVersion());
        payload.put("correlationId", workflowModel.getCorrelationId());

        // Compute queue name from sink parameter (reusing Conductor's logic)
        String sinkValue = (String) task.getInputData().get("sink");
        if (sinkValue == null || sinkValue.isEmpty()) {
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion("EVENT task requires 'sink' parameter");
            task.setEndTime(Workflow.currentTimeMillis());
            return;
        }

        String queueName = computeEventQueueName(sinkValue);

        // Set output data (includes payload and queue name)
        task.addOutput(payload);
        task.addOutput("event_produced", queueName);

        // Serialize payload to JSON
        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(task.getOutputData());
        } catch (JsonProcessingException e) {
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion("Failed to serialize event payload: " + e.getMessage());
            task.setEndTime(Workflow.currentTimeMillis());
            return;
        }

        // Call activity to publish event
        ActivityOptions options = ActivityOptions.newBuilder()
                .setStartToCloseTimeout(Duration.ofMinutes(5))
                .setRetryOptions(RetryOptions.newBuilder()
                        .setMaximumAttempts(3)
                        .build())
                .build();

        EventPublishActivity eventActivity = Workflow.newActivityStub(EventPublishActivity.class, options);

        try {
            eventActivity.publish(queueName, task.getTaskId(), payloadJson);
            task.setStatus(TaskModel.Status.COMPLETED);
            logger.info("EVENT task {} published to queue '{}'", taskRefName, queueName);
        } catch (Exception e) {
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion("Failed to publish event: " + e.getMessage());
            logger.error("EVENT task {} failed to publish to queue '{}': {}",
                    taskRefName, queueName, e.getMessage());
        }

        task.setEndTime(Workflow.currentTimeMillis());
    }

    /**
     * Compute the queue name from the sink parameter.
     * Follows Conductor's Event task logic for queue name computation.
     */
    private String computeEventQueueName(String sinkValue) {
        String queueName = sinkValue;

        if (sinkValue.startsWith("conductor")) {
            if ("conductor".equals(sinkValue)) {
                // conductor -> conductor:workflowName:taskRefName
                queueName = sinkValue + ":" + workflowModel.getWorkflowName() + ":" +
                        workflowModel.getTasks().stream()
                                .filter(t -> TaskType.EVENT.name().equals(t.getTaskType()))
                                .findFirst()
                                .map(TaskModel::getReferenceTaskName)
                                .orElse("event");
            } else if (sinkValue.startsWith("conductor:")) {
                // conductor:eventName -> conductor:workflowName:eventName
                queueName = "conductor:" + workflowModel.getWorkflowName() + ":" +
                        sinkValue.substring("conductor:".length());
            }
        }

        return queueName;
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
        // Exclude WAIT, JOIN, and SUB_WORKFLOW as they wait for async completion
        boolean hasProgressableSystemTasks = workflowModel.getTasks().stream()
                .anyMatch(t -> t.getStatus() == TaskModel.Status.IN_PROGRESS
                        && isSystemTask(t)
                        && !TaskType.WAIT.name().equals(t.getTaskType())
                        && !TaskType.HUMAN.name().equals(t.getTaskType())
                        && !TaskType.JOIN.name().equals(t.getTaskType())
                        && !TaskType.SUB_WORKFLOW.name().equals(t.getTaskType()));

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
     * Build ActivityOptions from Conductor TaskDef configuration.
     *
     * <p>Maps Conductor timeouts to Temporal:
     * <ul>
     *   <li>pollTimeoutSeconds → scheduleToStartTimeout (queue time)</li>
     *   <li>timeoutSeconds → startToCloseTimeout (execution time)</li>
     *   <li>responseTimeoutSeconds → heartbeatTimeout (worker liveness)</li>
     * </ul>
     */
    private ActivityOptions buildActivityOptions(TaskModel task) {
        TaskDef taskDef = metadataDao.getTaskDef(task.getTaskDefName());

        // Build retry options from TaskDef
        RetryOptions.Builder retryBuilder = RetryOptions.newBuilder();

        if (taskDef != null && taskDef.getRetryCount() > 0) {
            // Temporal maxAttempts = initial attempt + retries
            retryBuilder.setMaximumAttempts(taskDef.getRetryCount() + 1);

            if (taskDef.getRetryDelaySeconds() > 0) {
                retryBuilder.setInitialInterval(Duration.ofSeconds(taskDef.getRetryDelaySeconds()));

                // Map Conductor retry logic to Temporal backoff coefficient
                if (taskDef.getRetryLogic() == RetryLogic.EXPONENTIAL_BACKOFF) {
                    int scaleFactor = taskDef.getBackoffScaleFactor() != null
                            ? taskDef.getBackoffScaleFactor() : 2;
                    retryBuilder.setBackoffCoefficient(scaleFactor);
                } else if (taskDef.getRetryLogic() == RetryLogic.LINEAR_BACKOFF) {
                    int scaleFactor = taskDef.getBackoffScaleFactor() != null
                            ? taskDef.getBackoffScaleFactor() : 1;
                    retryBuilder.setBackoffCoefficient(scaleFactor);
                } else {
                    // FIXED - no backoff
                    retryBuilder.setBackoffCoefficient(1.0);
                }
            } else {
                retryBuilder.setInitialInterval(DEFAULT_RETRY_DELAY);
            }
        } else {
            retryBuilder.setMaximumAttempts(DEFAULT_RETRY_COUNT + 1);
            retryBuilder.setInitialInterval(DEFAULT_RETRY_DELAY);
        }

        // Build activity options with timeout mapping
        ActivityOptions.Builder optionsBuilder = ActivityOptions.newBuilder()
                .setRetryOptions(retryBuilder.build());

        // timeoutSeconds → startToCloseTimeout (execution time)
        if (taskDef != null && taskDef.getTimeoutSeconds() > 0) {
            optionsBuilder.setStartToCloseTimeout(Duration.ofSeconds(taskDef.getTimeoutSeconds()));
        } else {
            optionsBuilder.setStartToCloseTimeout(DEFAULT_START_TO_CLOSE_TIMEOUT);
        }

        // pollTimeoutSeconds → scheduleToStartTimeout (queue time)
        if (taskDef != null && taskDef.getPollTimeoutSeconds() != null
                && taskDef.getPollTimeoutSeconds() > 0) {
            optionsBuilder.setScheduleToStartTimeout(
                    Duration.ofSeconds(taskDef.getPollTimeoutSeconds()));
        }

        // responseTimeoutSeconds → heartbeatTimeout (worker liveness)
        // Only set if the activity implementation supports heartbeating
        if (taskDef != null && taskDef.getResponseTimeoutSeconds() > 0) {
            optionsBuilder.setHeartbeatTimeout(
                    Duration.ofSeconds(taskDef.getResponseTimeoutSeconds()));
        }

        return optionsBuilder.build();
    }

    /**
     * Start a worker task asynchronously - returns immediately.
     * Task completion is handled via Promise.handle callback which sets stateUpdated flag.
     */
    private void startWorkerTaskAsync(TaskModel task) {
        logger.debug("Starting async worker task: {} ({})",
                task.getReferenceTaskName(), task.getTaskDefName());

        task.setStatus(TaskModel.Status.IN_PROGRESS);
        task.setStartTime(Workflow.currentTimeMillis());

        ActivityOptions options = buildActivityOptions(task);
        ActivityStub activityStub = Workflow.newUntypedActivityStub(options);

        String activityType = task.getTaskDefName();
        String taskId = task.getTaskId();

        Promise<TaskExecutionResult> promise = activityStub.executeAsync(
                activityType,
                TaskExecutionResult.class,
                task.getReferenceTaskName(),
                task.getInputData() != null ? task.getInputData() : Collections.emptyMap()
        );

        pendingActivityTaskIds.add(taskId);

        // Event-driven completion via Promise.handle callback
        promise.handle((result, failure) -> {
            handleActivityCompletion(taskId, result, failure);
            return null;
        });
    }

    /**
     * Handle activity completion - called by Promise.handle callback.
     * Updates task state and sets stateUpdated flag to wake up main loop.
     */
    private void handleActivityCompletion(String taskId, TaskExecutionResult result, Throwable failure) {
        TaskModel task = getTaskById(taskId);
        if (task == null) {
            logger.warn("Activity completed but task not found: {}", taskId);
            pendingActivityTaskIds.remove(taskId);
            stateUpdated = true;
            return;
        }

        if (failure != null) {
            logger.debug("Activity failed for task {}: {}", task.getReferenceTaskName(), failure.getMessage());
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion(failure.getMessage());
        } else {
            task.setOutputData(result.getOutput());
            if ("COMPLETED".equals(result.getStatus())) {
                task.setStatus(TaskModel.Status.COMPLETED);
            } else {
                task.setStatus(TaskModel.Status.FAILED);
                task.setReasonForIncompletion(result.getFailureReason());
            }
        }
        task.setEndTime(Workflow.currentTimeMillis());

        pendingActivityTaskIds.remove(taskId);
        stateUpdated = true;  // Signal main loop to wake up
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
            ActivityOptions options = buildActivityOptions(task);
            ActivityStub activityStub = Workflow.newUntypedActivityStub(options);

            String activityType = task.getTaskDefName();
            TaskExecutionResult result = activityStub.execute(
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
                case "completeTaskById":
                    String taskId = encodedArgs.get(0, String.class);
                    @SuppressWarnings("unchecked")
                    Map<String, Object> taskOutput = encodedArgs.get(1, Map.class);
                    completeTaskById(taskId, taskOutput);
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

    private void completeTaskById(String taskId, Map<String, Object> output) {
        logger.info("Received signal to complete task by ID: {}", taskId);
        TaskModel task = workflowModel.getTasks().stream()
                .filter(t -> taskId.equals(t.getTaskId()))
                .findFirst()
                .orElse(null);
        if (task != null) {
            pendingSignals.put(task.getReferenceTaskName(), output);
        } else {
            logger.warn("Task not found for ID: {}", taskId);
        }
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
        return pendingActivityTaskIds.isEmpty()
                && pendingChildWorkflowTaskIds.isEmpty()
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

        // Capture checkpoint state with optimized task list
        ContinueAsNewCheckpoint checkpoint = new ContinueAsNewCheckpoint();
        checkpoint.setCompletedTasks(tasksToSnapshots(getEssentialTasksForCheckpoint()));
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
        List<TaskModel> restoredTasks = snapshotsToTasks(checkpoint.getCompletedTasks());
        workflowModel.getTasks().addAll(restoredTasks);

        // Re-link WorkflowTask on restored tasks - needed for DO_WHILE loop conditions
        relinkWorkflowTasks(restoredTasks);

        if (checkpoint.getVariables() != null) {
            workflowModel.setVariables(new HashMap<>(checkpoint.getVariables()));
        }

        workflowModel.setCorrelationId(checkpoint.getCorrelationId());
        workflowModel.setPriority(checkpoint.getPriority());
        workflowModel.setOwnerApp(checkpoint.getOwnerApp());
        workflowModel.setCreatedBy(checkpoint.getCreatedBy());
        workflowModel.setCreateTime(checkpoint.getOriginalCreateTime());

        pendingSignals.putAll(checkpoint.getPendingSignals());

        // Restore pending timers using callback-based approach
        for (PendingTimer timer : checkpoint.getPendingTimers()) {
            if (timer.getRemainingDurationMs() > 0) {
                startWaitTimer(timer.getTaskRefName(), Duration.ofMillis(timer.getRemainingDurationMs()));
                logger.debug("Recreated timer for task {} with {}ms remaining",
                        timer.getTaskRefName(), timer.getRemainingDurationMs());
            }
        }
    }

    /**
     * Re-link WorkflowTask metadata on restored tasks after continue-as-new.
     *
     * <p>The WorkflowTask contains essential metadata like loop conditions for DO_WHILE
     * that isn't preserved in TaskSnapshot. This method looks up the WorkflowTask from
     * the workflow definition and sets it on each restored task.
     */
    private void relinkWorkflowTasks(List<TaskModel> tasks) {
        WorkflowDef workflowDef = workflowModel.getWorkflowDefinition();
        if (workflowDef == null) {
            logger.warn("Cannot relink WorkflowTasks: workflow definition is null");
            return;
        }

        for (TaskModel task : tasks) {
            WorkflowTask workflowTask = workflowDef.getTaskByRefName(task.getReferenceTaskName());
            if (workflowTask != null) {
                task.setWorkflowTask(workflowTask);
                logger.debug("Relinked WorkflowTask for task: {} (type: {})",
                        task.getReferenceTaskName(), task.getTaskType());
            } else {
                // For dynamically created tasks (e.g., loop iterations), try to find parent
                String refName = task.getReferenceTaskName();
                int lastUnderscore = refName.lastIndexOf('_');
                if (lastUnderscore > 0) {
                    String parentRefName = refName.substring(0, lastUnderscore);
                    WorkflowTask parentTask = workflowDef.getTaskByRefName(parentRefName);
                    if (parentTask != null) {
                        task.setWorkflowTask(parentTask);
                        logger.debug("Relinked WorkflowTask for dynamic task: {} -> parent: {}",
                                refName, parentRefName);
                    }
                }
            }
        }
    }

    /**
     * Get the essential tasks needed for checkpoint restoration.
     * This filters out completed loop iteration tasks to reduce checkpoint size,
     * keeping only tasks necessary for the workflow to continue correctly.
     */
    private List<TaskModel> getEssentialTasksForCheckpoint() {
        // Use a map to track the latest iteration of each loop task
        Map<String, TaskModel> latestLoopTasks = new HashMap<>();
        List<TaskModel> nonLoopTasks = new ArrayList<>();

        for (TaskModel task : workflowModel.getTasks()) {
            String refName = task.getReferenceTaskName();

            // Always keep system tasks (FORK, JOIN, DO_WHILE, SWITCH, etc.)
            if (isSystemTask(task)) {
                nonLoopTasks.add(task);
                continue;
            }

            // Keep IN_PROGRESS or SCHEDULED tasks
            if (task.getStatus() == TaskModel.Status.IN_PROGRESS
                    || task.getStatus() == TaskModel.Status.SCHEDULED) {
                nonLoopTasks.add(task);
                continue;
            }

            // For loop iteration tasks (e.g., "task_ref__1", "task_ref__2"), keep only the latest
            if (refName.contains("__")) {
                String baseRefName = refName.substring(0, refName.lastIndexOf("__"));
                // Always overwrite - later tasks have higher iteration numbers
                latestLoopTasks.put(baseRefName, task);
            } else {
                // Non-loop completed tasks
                nonLoopTasks.add(task);
            }
        }

        // Combine non-loop tasks with latest loop iterations
        List<TaskModel> essential = new ArrayList<>(nonLoopTasks);
        essential.addAll(latestLoopTasks.values());

        logger.info("Checkpoint optimization: {} total tasks -> {} essential tasks",
                workflowModel.getTasks().size(), essential.size());

        return essential;
    }

    private List<PendingTimer> capturePendingTimers() {
        List<PendingTimer> timers = new ArrayList<>();
        long currentTime = Workflow.currentTimeMillis();

        for (String taskRefName : pendingTimerTaskRefs) {
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
            // Don't preserve inputData in checkpoint - it's not needed for expression evaluation
            // and can be very large with payload-heavy workflows
            snapshot.setInputData(new HashMap<>());
            // Truncate output data to prevent checkpoint from exceeding Temporal payload limits
            snapshot.setOutputData(truncateDataForCheckpoint(task.getOutputData()));
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

    /**
     * Truncate data for checkpoint serialization to prevent exceeding Temporal payload limits.
     * This is more aggressive than query truncation since checkpoints need to fit in ~4MB.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> truncateDataForCheckpoint(Map<String, Object> data) {
        if (data == null) {
            return new HashMap<>();
        }
        Map<String, Object> truncated = new HashMap<>();
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof String) {
                String strValue = (String) value;
                // More aggressive truncation for checkpoints - 512 bytes max per string
                if (strValue.length() > 512) {
                    truncated.put(entry.getKey(), strValue.substring(0, 512) + "...[CHECKPOINT_TRUNCATED]");
                } else {
                    truncated.put(entry.getKey(), value);
                }
            } else if (value instanceof Map) {
                truncated.put(entry.getKey(), truncateDataForCheckpoint((Map<String, Object>) value));
            } else {
                truncated.put(entry.getKey(), value);
            }
        }
        return truncated;
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
