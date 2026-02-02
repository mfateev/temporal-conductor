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

package io.temporal.conductor.handler.extension;

import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import io.temporal.conductor.handler.ExecutionMode;
import io.temporal.conductor.handler.TaskExecutionContext;
import io.temporal.conductor.handler.TaskTypeHandler;
import io.temporal.conductor.workflow.model.TaskExecutionResult;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adapter that wraps a Conductor WorkflowSystemTask as a TaskTypeHandler.
 *
 * <p>Extension tasks are executed via Temporal activities for safety and durability.
 * This adapter delegates execution to the {@code ExtensionTaskExecutionActivity},
 * which runs the actual WorkflowSystemTask implementation.
 *
 * <p>All extension tasks use ACTIVITY execution mode, ensuring:
 * <ul>
 *   <li>Proper retry semantics via Temporal activities</li>
 *   <li>Isolation of non-deterministic operations from workflow code</li>
 *   <li>Durability guarantees through activity completion</li>
 * </ul>
 */
public class ExtensionTaskAdapter implements TaskTypeHandler {

    private static final Logger logger = LoggerFactory.getLogger(ExtensionTaskAdapter.class);

    /**
     * Activity type name for extension task execution.
     * This must match the activity method name in ExtensionTaskExecutionActivity.
     */
    public static final String EXTENSION_ACTIVITY_TYPE = "extension-task-execute";

    private final String taskType;

    /**
     * Create an adapter for a specific task type.
     *
     * @param taskType the task type this adapter handles
     */
    public ExtensionTaskAdapter(String taskType) {
        this.taskType = taskType;
    }

    @Override
    public String getTaskType() {
        return taskType;
    }

    @Override
    public ExecutionMode getExecutionMode(TaskModel task) {
        // All extension tasks run via activity for safety
        return ExecutionMode.ACTIVITY;
    }

    @Override
    public void execute(TaskModel task, WorkflowModel workflow, TaskExecutionContext context) {
        String taskRefName = task.getReferenceTaskName();
        String taskId = task.getTaskId();

        if (task.getStatus() == TaskModel.Status.SCHEDULED) {
            task.setStatus(TaskModel.Status.IN_PROGRESS);
            task.setStartTime(context.currentTimeMillis());
        }

        // Check if activity already pending
        if (context.isActivityPending(taskId)) {
            return;
        }

        logger.info("Executing extension task {} via activity: type={}", taskRefName, taskType);

        // Build activity input - the activity needs the task type to look up the implementation
        Map<String, Object> activityInput = new HashMap<>();
        activityInput.put("taskType", taskType);
        activityInput.put("taskRefName", taskRefName);
        activityInput.put("inputData", task.getInputData() != null ? task.getInputData() : Collections.emptyMap());

        // Execute via activity asynchronously
        context.executeActivityAsync(
                EXTENSION_ACTIVITY_TYPE,
                taskId,
                taskRefName,
                activityInput,
                (result, failure) -> handleActivityCompletion(task, result, failure, context));
    }

    /**
     * Handle activity completion callback.
     */
    private void handleActivityCompletion(
            TaskModel task,
            TaskExecutionResult result,
            Throwable failure,
            TaskExecutionContext context) {
        if (failure != null) {
            logger.error("Extension task {} failed: {}", task.getReferenceTaskName(), failure.getMessage());
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion(failure.getMessage());
        } else {
            task.setOutputData(result.getOutput());
            if ("COMPLETED".equals(result.getStatus())) {
                task.setStatus(TaskModel.Status.COMPLETED);
                logger.info("Extension task {} completed successfully", task.getReferenceTaskName());
            } else {
                task.setStatus(TaskModel.Status.FAILED);
                task.setReasonForIncompletion(result.getFailureReason());
                logger.warn("Extension task {} failed: {}", task.getReferenceTaskName(), result.getFailureReason());
            }
        }
        task.setEndTime(context.currentTimeMillis());
        context.markStateUpdated();
    }
}
