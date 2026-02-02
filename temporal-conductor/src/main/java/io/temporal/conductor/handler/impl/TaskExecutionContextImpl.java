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

package io.temporal.conductor.handler.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.execution.DeciderService;
import com.netflix.conductor.model.TaskModel;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.conductor.executor.InMemoryMetadataDAO;
import io.temporal.conductor.executor.SystemTaskExecutor;
import io.temporal.conductor.handler.TaskExecutionContext;
import io.temporal.conductor.workflow.model.ConductorWorkflowInput;
import io.temporal.conductor.workflow.model.ConductorWorkflowOutput;
import io.temporal.conductor.workflow.model.TaskExecutionResult;
import io.temporal.workflow.ActivityStub;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.ChildWorkflowStub;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.slf4j.Logger;

/**
 * Implementation of TaskExecutionContext that provides access to Temporal primitives.
 *
 * <p>This class bridges the gap between task handlers and the Temporal workflow API,
 * allowing handlers to use timers, activities, and child workflows without directly
 * depending on Temporal classes.
 */
public class TaskExecutionContextImpl implements TaskExecutionContext {

    private static final Logger logger = Workflow.getLogger(TaskExecutionContextImpl.class);

    private final ObjectMapper objectMapper;
    private final DeciderService deciderService;
    private final SystemTaskExecutor systemTaskExecutor;
    private final InMemoryMetadataDAO metadataDao;
    private final ConductorWorkflowInput workflowInput;

    // Mutable state managed by the workflow
    private final Set<String> pendingActivityTaskIds;
    private final Set<String> pendingTimerTaskRefs;
    private final Set<String> pendingChildWorkflowTaskIds;
    private final Map<String, Map<String, Object>> pendingSignals;
    private final Consumer<Boolean> stateUpdateCallback;

    // Default timeouts
    private static final Duration DEFAULT_START_TO_CLOSE_TIMEOUT = Duration.ofMinutes(10);

    public TaskExecutionContextImpl(
            ObjectMapper objectMapper,
            DeciderService deciderService,
            SystemTaskExecutor systemTaskExecutor,
            InMemoryMetadataDAO metadataDao,
            ConductorWorkflowInput workflowInput,
            Set<String> pendingActivityTaskIds,
            Set<String> pendingTimerTaskRefs,
            Set<String> pendingChildWorkflowTaskIds,
            Map<String, Map<String, Object>> pendingSignals,
            Consumer<Boolean> stateUpdateCallback) {
        this.objectMapper = objectMapper;
        this.deciderService = deciderService;
        this.systemTaskExecutor = systemTaskExecutor;
        this.metadataDao = metadataDao;
        this.workflowInput = workflowInput;
        this.pendingActivityTaskIds = pendingActivityTaskIds;
        this.pendingTimerTaskRefs = pendingTimerTaskRefs;
        this.pendingChildWorkflowTaskIds = pendingChildWorkflowTaskIds;
        this.pendingSignals = pendingSignals;
        this.stateUpdateCallback = stateUpdateCallback;
    }

    // ==================== Timer Operations ====================

    @Override
    public void startTimer(String taskRefName, Duration duration, Runnable onComplete) {
        pendingTimerTaskRefs.add(taskRefName);
        Promise<Void> timer = Workflow.newTimer(duration);
        logger.debug("Started timer for task {} with duration {}", taskRefName, duration);

        timer.handle((result, failure) -> {
            pendingTimerTaskRefs.remove(taskRefName);
            if (failure == null) {
                onComplete.run();
            } else {
                logger.warn("Timer failed for task {}: {}", taskRefName, failure.getMessage());
            }
            return null;
        });
    }

    @Override
    public boolean isTimerPending(String taskRefName) {
        return pendingTimerTaskRefs.contains(taskRefName);
    }

    // ==================== Activity Operations ====================

    @Override
    public void executeActivityAsync(
            String activityType,
            String taskId,
            String taskRefName,
            Map<String, Object> inputData,
            BiConsumer<TaskExecutionResult, Throwable> onComplete) {

        logger.debug("Starting async activity {} for task {}", activityType, taskRefName);

        ActivityOptions options = ActivityOptions.newBuilder()
                .setStartToCloseTimeout(DEFAULT_START_TO_CLOSE_TIMEOUT)
                .setRetryOptions(RetryOptions.newBuilder()
                        .setMaximumAttempts(3)
                        .build())
                .build();

        ActivityStub activityStub = Workflow.newUntypedActivityStub(options);

        Promise<TaskExecutionResult> promise = activityStub.executeAsync(
                activityType,
                TaskExecutionResult.class,
                taskRefName,
                inputData != null ? inputData : Collections.emptyMap()
        );

        pendingActivityTaskIds.add(taskId);

        promise.handle((result, failure) -> {
            pendingActivityTaskIds.remove(taskId);
            onComplete.accept(result, failure);
            return null;
        });
    }

    @Override
    public void executeActivitySync(
            String activityType,
            String taskRefName,
            Map<String, Object> inputData,
            TaskModel task) {

        logger.debug("Executing sync activity {} for task {}", activityType, taskRefName);

        ActivityOptions options = ActivityOptions.newBuilder()
                .setStartToCloseTimeout(DEFAULT_START_TO_CLOSE_TIMEOUT)
                .setRetryOptions(RetryOptions.newBuilder()
                        .setMaximumAttempts(3)
                        .build())
                .build();

        ActivityStub activityStub = Workflow.newUntypedActivityStub(options);

        try {
            TaskExecutionResult result = activityStub.execute(
                    activityType,
                    TaskExecutionResult.class,
                    taskRefName,
                    inputData != null ? inputData : Collections.emptyMap()
            );

            task.setOutputData(result.getOutput());
            if ("COMPLETED".equals(result.getStatus())) {
                task.setStatus(TaskModel.Status.COMPLETED);
            } else {
                task.setStatus(TaskModel.Status.FAILED);
                task.setReasonForIncompletion(result.getFailureReason());
            }
        } catch (Exception e) {
            logger.error("Activity {} failed: {}", activityType, e.getMessage());
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion(e.getMessage());
        }
        task.setEndTime(Workflow.currentTimeMillis());
    }

    @Override
    public boolean isActivityPending(String taskId) {
        return pendingActivityTaskIds.contains(taskId);
    }

    // ==================== Child Workflow Operations ====================

    @Override
    public void startChildWorkflow(
            String childWorkflowId,
            String childWorkflowType,
            ConductorWorkflowInput childInput,
            ChildWorkflowOptions options,
            String taskId,
            BiConsumer<Map<String, Object>, Throwable> onComplete) {

        logger.info("Starting child workflow {} of type {}", childWorkflowId, childWorkflowType);

        ChildWorkflowStub childStub = Workflow.newUntypedChildWorkflowStub(childWorkflowType, options);
        Promise<ConductorWorkflowOutput> promise = childStub.executeAsync(
                ConductorWorkflowOutput.class, childInput);

        pendingChildWorkflowTaskIds.add(taskId);

        promise.handle((result, failure) -> {
            pendingChildWorkflowTaskIds.remove(taskId);
            if (failure != null) {
                onComplete.accept(null, failure);
            } else {
                onComplete.accept(result.getOutput(), null);
            }
            return null;
        });
    }

    @Override
    public void startFireAndForgetChildWorkflow(
            String childWorkflowId,
            String childWorkflowType,
            ConductorWorkflowInput childInput,
            ChildWorkflowOptions options) {

        logger.info("Starting fire-and-forget child workflow {} of type {}", childWorkflowId, childWorkflowType);

        ChildWorkflowStub childStub = Workflow.newUntypedChildWorkflowStub(childWorkflowType, options);
        // Start without tracking - fire and forget
        childStub.executeAsync(ConductorWorkflowOutput.class, childInput);
    }

    @Override
    public boolean isChildWorkflowPending(String taskId) {
        return pendingChildWorkflowTaskIds.contains(taskId);
    }

    // ==================== Signal Operations ====================

    @Override
    public Optional<Map<String, Object>> getPendingSignal(String taskRefName) {
        Map<String, Object> signal = pendingSignals.get(taskRefName);
        return Optional.ofNullable(signal);
    }

    // ==================== Workflow State ====================

    @Override
    public DeciderService getDeciderService() {
        return deciderService;
    }

    @Override
    public SystemTaskExecutor getSystemTaskExecutor() {
        return systemTaskExecutor;
    }

    @Override
    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    @Override
    public ConductorWorkflowInput getWorkflowInput() {
        return workflowInput;
    }

    @Override
    public long currentTimeMillis() {
        return Workflow.currentTimeMillis();
    }

    @Override
    public String getWorkflowId() {
        return Workflow.getInfo().getWorkflowId();
    }

    @Override
    public String getTaskQueue() {
        return Workflow.getInfo().getTaskQueue();
    }

    @Override
    public Optional<WorkflowDef> getWorkflowDef(String workflowName, Integer version) {
        if (version != null) {
            return metadataDao.getWorkflowDef(workflowName, version);
        }
        return metadataDao.getLatestWorkflowDef(workflowName);
    }

    @Override
    public void markStateUpdated() {
        stateUpdateCallback.accept(true);
    }
}
