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

package io.temporal.conductor.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.execution.DeciderService;
import com.netflix.conductor.model.TaskModel;
import io.temporal.conductor.executor.SystemTaskExecutor;
import io.temporal.conductor.workflow.model.ConductorWorkflowInput;
import io.temporal.conductor.workflow.model.TaskExecutionResult;
import io.temporal.workflow.ChildWorkflowOptions;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * Provides access to Temporal primitives and workflow state for task execution.
 *
 * <p>This interface abstracts the Temporal workflow API, allowing task handlers
 * to interact with timers, activities, child workflows, and signals without
 * directly depending on Temporal's workflow classes.
 *
 * <p>The implementation is provided by the workflow and passed to handlers
 * during task execution.
 */
public interface TaskExecutionContext {

    // ==================== Timer Operations ====================

    /**
     * Start a timer for a task.
     * The callback is invoked when the timer fires.
     *
     * @param taskRefName the task reference name (used for tracking)
     * @param duration the timer duration
     * @param onComplete callback invoked when timer completes
     */
    void startTimer(String taskRefName, Duration duration, Runnable onComplete);

    /**
     * Check if a timer is pending for a task.
     *
     * @param taskRefName the task reference name
     * @return true if a timer is pending
     */
    boolean isTimerPending(String taskRefName);

    // ==================== Activity Operations ====================

    /**
     * Execute an activity asynchronously.
     * The callback is invoked when the activity completes.
     *
     * @param activityType the activity type name
     * @param taskId the task ID (used for tracking)
     * @param taskRefName the task reference name
     * @param conductorTaskType the Conductor task type (e.g., JSON_JQ_TRANSFORM, SIMPLE)
     * @param inputData the activity input data
     * @param onComplete callback invoked with result or failure
     */
    void executeActivityAsync(
            String activityType,
            String taskId,
            String taskRefName,
            String conductorTaskType,
            Map<String, Object> inputData,
            BiConsumer<TaskExecutionResult, Throwable> onComplete);

    /**
     * Execute an activity synchronously (blocking).
     *
     * @param activityType the activity type name
     * @param taskRefName the task reference name
     * @param inputData the activity input data
     * @param task the task model to update with results
     */
    void executeActivitySync(
            String activityType,
            String taskRefName,
            Map<String, Object> inputData,
            TaskModel task);

    /**
     * Check if an activity is pending for a task.
     *
     * @param taskId the task ID
     * @return true if an activity is pending
     */
    boolean isActivityPending(String taskId);

    // ==================== Child Workflow Operations ====================

    /**
     * Start a child workflow and wait for completion.
     * The callback is invoked when the child workflow completes.
     *
     * @param childWorkflowId the child workflow ID
     * @param childWorkflowType the child workflow type name
     * @param childInput the child workflow input
     * @param options child workflow options
     * @param taskId the parent task ID (used for tracking)
     * @param onComplete callback invoked with result or failure
     */
    void startChildWorkflow(
            String childWorkflowId,
            String childWorkflowType,
            ConductorWorkflowInput childInput,
            ChildWorkflowOptions options,
            String taskId,
            BiConsumer<Map<String, Object>, Throwable> onComplete);

    /**
     * Start a child workflow without waiting (fire-and-forget).
     *
     * @param childWorkflowId the child workflow ID
     * @param childWorkflowType the child workflow type name
     * @param childInput the child workflow input
     * @param options child workflow options
     */
    void startFireAndForgetChildWorkflow(
            String childWorkflowId,
            String childWorkflowType,
            ConductorWorkflowInput childInput,
            ChildWorkflowOptions options);

    /**
     * Check if a child workflow is pending for a task.
     *
     * @param taskId the task ID
     * @return true if a child workflow is pending
     */
    boolean isChildWorkflowPending(String taskId);

    // ==================== Signal Operations ====================

    /**
     * Check if a signal has been received for a task.
     *
     * @param taskRefName the task reference name
     * @return the signal data if received, empty otherwise
     */
    Optional<Map<String, Object>> getPendingSignal(String taskRefName);

    // ==================== Workflow State ====================

    /**
     * Get the DeciderService for task scheduling decisions.
     *
     * @return the decider service
     */
    DeciderService getDeciderService();

    /**
     * Get the SystemTaskExecutor for executing system tasks.
     *
     * @return the system task executor
     */
    SystemTaskExecutor getSystemTaskExecutor();

    /**
     * Get the ObjectMapper for JSON serialization.
     *
     * @return the object mapper
     */
    ObjectMapper getObjectMapper();

    /**
     * Get the workflow input configuration.
     *
     * @return the workflow input
     */
    ConductorWorkflowInput getWorkflowInput();

    /**
     * Get the current workflow time (deterministic).
     *
     * @return current time in milliseconds
     */
    long currentTimeMillis();

    /**
     * Get the current workflow ID.
     *
     * @return the workflow ID
     */
    String getWorkflowId();

    /**
     * Get the task queue name.
     *
     * @return the task queue
     */
    String getTaskQueue();

    /**
     * Look up a workflow definition from metadata.
     *
     * @param workflowName the workflow name
     * @param version the version (null for latest)
     * @return the workflow definition if found
     */
    Optional<WorkflowDef> getWorkflowDef(String workflowName, Integer version);

    /**
     * Mark that workflow state has been updated.
     * This signals the main scheduling loop to wake up.
     */
    void markStateUpdated();
}
