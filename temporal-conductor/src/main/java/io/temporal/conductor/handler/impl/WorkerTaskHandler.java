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

import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import io.temporal.conductor.handler.ExecutionMode;
import io.temporal.conductor.handler.TaskExecutionContext;
import io.temporal.conductor.handler.TaskTypeHandler;
import io.temporal.conductor.util.TaskNameParser;
import io.temporal.conductor.util.TaskNameParser.ParsedTaskName;
import io.temporal.conductor.workflow.model.TaskExecutionResult;
import java.util.Collections;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for worker tasks (SIMPLE, HTTP, and custom task types).
 *
 * <p>Worker tasks are executed via Temporal activities. The activity name
 * is derived from the task definition name (taskDefName).
 *
 * <p>This is the default handler used for any task type not registered
 * with a specific handler.
 *
 * <p>Supported task types:
 * <ul>
 *   <li>SIMPLE - Generic worker task</li>
 *   <li>HTTP - HTTP request task</li>
 *   <li>Custom task types - Any user-defined task type</li>
 * </ul>
 */
public class WorkerTaskHandler implements TaskTypeHandler {

    private static final Logger logger = LoggerFactory.getLogger(WorkerTaskHandler.class);

    /**
     * Known worker task types that use activity execution.
     */
    public static final Set<String> KNOWN_WORKER_TYPES = Set.of(
            TaskType.SIMPLE.name(),
            TaskType.HTTP.name()
    );

    private final String taskType;

    /**
     * Create a handler for a specific task type.
     *
     * @param taskType the task type to handle
     */
    public WorkerTaskHandler(String taskType) {
        this.taskType = taskType;
    }

    /**
     * Create a default worker task handler.
     * The task type is returned as "SIMPLE" but this handler can handle any task type.
     */
    public WorkerTaskHandler() {
        this.taskType = TaskType.SIMPLE.name();
    }

    @Override
    public String getTaskType() {
        return taskType;
    }

    @Override
    public ExecutionMode getExecutionMode(TaskModel task) {
        return ExecutionMode.ACTIVITY;
    }

    @Override
    public void execute(TaskModel task, WorkflowModel workflow, TaskExecutionContext context) {
        String taskRefName = task.getReferenceTaskName();
        String taskId = task.getTaskId();

        logger.debug("Executing worker task: {} ({})", taskRefName, task.getTaskDefName());

        if (task.getStatus() == TaskModel.Status.SCHEDULED) {
            task.setStatus(TaskModel.Status.IN_PROGRESS);
            task.setStartTime(context.currentTimeMillis());
        }

        // Check if activity already pending
        if (context.isActivityPending(taskId)) {
            return;
        }

        // Parse task name for optional task queue suffix (e.g., "process_order@external-workers")
        String taskDefName = task.getTaskDefName();
        ParsedTaskName parsed = TaskNameParser.parse(taskDefName);

        // Activity type is the base name (without @taskQueue suffix)
        String activityType = parsed.baseName();
        String taskQueue = parsed.taskQueue();

        if (taskQueue != null) {
            logger.debug("Routing task {} to external task queue: {}", taskRefName, taskQueue);
        }

        // Execute activity asynchronously
        // Pass: taskRefName, taskType, inputData, taskQueue
        context.executeActivityAsync(
                activityType,
                taskId,
                taskRefName,
                task.getTaskType(),
                task.getInputData() != null ? task.getInputData() : Collections.emptyMap(),
                taskQueue,
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
            logger.debug("Activity failed for task {}: {}",
                    task.getReferenceTaskName(), failure.getMessage());
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
        task.setEndTime(context.currentTimeMillis());
        context.markStateUpdated();
    }

    /**
     * Check if a task type is a known worker task type.
     *
     * @param taskType the task type to check
     * @return true if this is a known worker task type
     */
    public static boolean isKnownWorkerType(String taskType) {
        return KNOWN_WORKER_TYPES.contains(taskType);
    }
}
