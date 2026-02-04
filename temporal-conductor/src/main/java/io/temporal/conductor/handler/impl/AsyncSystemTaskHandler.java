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
 * Handler for system tasks that require async re-evaluation.
 *
 * <p>This handler manages:
 * <ul>
 *   <li>DO_WHILE - Loop control that may need multiple iterations</li>
 *   <li>JOIN - Waits for parallel branches to complete</li>
 *   <li>EXCLUSIVE_JOIN - Waits for any one branch to complete</li>
 * </ul>
 *
 * <p>These tasks don't complete on first execution - they remain IN_PROGRESS
 * and need to be re-evaluated on each scheduling loop iteration until their
 * completion conditions are met.
 */
public class AsyncSystemTaskHandler implements TaskTypeHandler {

    private static final Logger logger = LoggerFactory.getLogger(AsyncSystemTaskHandler.class);

    /**
     * Task types that need async re-evaluation.
     */
    public static final Set<String> SUPPORTED_TYPES = Set.of(
            TaskType.DO_WHILE.name(),
            TaskType.JOIN.name(),
            TaskType.EXCLUSIVE_JOIN.name()
    );

    private final String taskType;

    /**
     * Create a handler for a specific async system task type.
     *
     * @param taskType the task type to handle
     */
    public AsyncSystemTaskHandler(String taskType) {
        this.taskType = taskType;
    }

    @Override
    public String getTaskType() {
        return taskType;
    }

    @Override
    public ExecutionMode getExecutionMode(TaskModel task) {
        return ExecutionMode.ASYNC_REEVAL;
    }

    @Override
    public void execute(TaskModel task, WorkflowModel workflow, TaskExecutionContext context) {
        boolean isDoWhile = TaskType.DO_WHILE.name().equals(task.getTaskType());

        if (isDoWhile) {
            logger.info("DO_WHILE execute: task={}, status={}, iteration={}",
                    task.getReferenceTaskName(), task.getStatus(), task.getIteration());
        } else {
            logger.debug("Executing async system task: {} ({})",
                    task.getReferenceTaskName(), task.getTaskType());
        }

        if (task.getStartTime() == 0L) {
            task.setStartTime(context.currentTimeMillis());
        }

        // Delegate to SystemTaskExecutor for actual execution
        boolean statusChanged = context.getSystemTaskExecutor().execute(workflow, task);

        if (isDoWhile) {
            logger.info("DO_WHILE after execute: task={}, statusChanged={}, status={}, iteration={}",
                    task.getReferenceTaskName(), statusChanged, task.getStatus(), task.getIteration());
        } else {
            logger.debug("Async system task {} statusChanged={}, status: {}",
                    task.getReferenceTaskName(), statusChanged, task.getStatus());
        }

        if (task.getStatus().isTerminal() && task.getEndTime() == 0L) {
            task.setEndTime(context.currentTimeMillis());
        }
    }

    /**
     * Check if a task type is handled by AsyncSystemTaskHandler.
     *
     * @param taskType the task type to check
     * @return true if this handler supports the task type
     */
    public static boolean supports(String taskType) {
        return SUPPORTED_TYPES.contains(taskType);
    }
}
