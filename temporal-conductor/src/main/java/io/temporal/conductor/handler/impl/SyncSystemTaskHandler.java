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
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for synchronous system tasks that complete immediately.
 *
 * <p>This handler delegates to Conductor's SystemTaskExecutor for:
 * <ul>
 *   <li>FORK/FORK_JOIN - Parallel branch creation</li>
 *   <li>SWITCH - Conditional branching</li>
 *   <li>SET_VARIABLE - Variable assignment</li>
 *   <li>TERMINATE - Workflow termination</li>
 *   <li>NOOP - No operation placeholder</li>
 * </ul>
 *
 * <p>Note: JOIN and DO_WHILE are handled separately as they require
 * async re-evaluation (ASYNC_REEVAL mode).
 */
public class SyncSystemTaskHandler implements TaskTypeHandler {

    private static final Logger logger = LoggerFactory.getLogger(SyncSystemTaskHandler.class);

    /**
     * Task types handled by this handler.
     * These are synchronous system tasks that complete in a single execution.
     */
    public static final Set<String> SUPPORTED_TYPES = Set.of(
            TaskType.FORK_JOIN.name(),
            TaskType.TASK_TYPE_FORK,  // "FORK" - actual type used in workflows
            TaskType.SWITCH.name(),
            TaskType.SET_VARIABLE.name(),
            TaskType.TERMINATE.name(),
            "NOOP"
    );

    private final String taskType;

    /**
     * Create a handler for a specific task type.
     *
     * @param taskType the task type to handle
     */
    public SyncSystemTaskHandler(String taskType) {
        this.taskType = taskType;
    }

    @Override
    public String getTaskType() {
        return taskType;
    }

    @Override
    public ExecutionMode getExecutionMode(TaskModel task) {
        return ExecutionMode.SYNC;
    }

    @Override
    public void execute(TaskModel task, WorkflowModel workflow, TaskExecutionContext context) {
        logger.debug("Executing sync system task: {} ({})",
                task.getReferenceTaskName(), task.getTaskType());

        if (task.getStartTime() == 0L) {
            task.setStartTime(context.currentTimeMillis());
        }

        // Delegate to SystemTaskExecutor
        context.getSystemTaskExecutor().execute(workflow, task);

        if (task.getStatus().isTerminal() && task.getEndTime() == 0L) {
            task.setEndTime(context.currentTimeMillis());
        }

        // Handle TERMINATE task special case - update workflow status
        if (TaskType.TERMINATE.name().equals(task.getTaskType())
                && task.getStatus() == TaskModel.Status.COMPLETED) {
            Object terminationStatus = task.getInputData().get("terminationStatus");
            if ("COMPLETED".equals(terminationStatus)) {
                workflow.setStatus(WorkflowModel.Status.COMPLETED);
            } else if ("FAILED".equals(terminationStatus)) {
                workflow.setStatus(WorkflowModel.Status.FAILED);
            } else {
                workflow.setStatus(WorkflowModel.Status.TERMINATED);
            }
        }

        logger.debug("Sync system task {} completed with status: {}",
                task.getReferenceTaskName(), task.getStatus());
    }

    /**
     * Check if a task type is handled by SyncSystemTaskHandler.
     *
     * @param taskType the task type to check
     * @return true if this handler supports the task type
     */
    public static boolean supports(String taskType) {
        return SUPPORTED_TYPES.contains(taskType);
    }
}
