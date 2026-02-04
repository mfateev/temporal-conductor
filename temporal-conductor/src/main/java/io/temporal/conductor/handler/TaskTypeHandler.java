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

import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

/**
 * Strategy interface for handling different Conductor task types.
 *
 * <p>Each implementation handles a specific task type (e.g., WAIT, HUMAN, SUB_WORKFLOW)
 * or a category of task types (e.g., all sync system tasks). The handler determines
 * both the execution mode and the actual execution logic.
 *
 * <p>Handlers are registered with {@link TaskTypeHandlerRegistry} and looked up
 * by task type during workflow execution.
 *
 * <p>Example implementation:
 * <pre>{@code
 * public class WaitTaskHandler implements TaskTypeHandler {
 *     @Override
 *     public String getTaskType() {
 *         return TaskType.WAIT.name();
 *     }
 *
 *     @Override
 *     public ExecutionMode getExecutionMode(TaskModel task) {
 *         // Check if WAIT has a duration - if so, use timer; otherwise wait for signal
 *         return hasDuration(task) ? ExecutionMode.TIMER : ExecutionMode.WAIT_FOR_SIGNAL;
 *     }
 *
 *     @Override
 *     public void execute(TaskModel task, WorkflowModel workflow, TaskExecutionContext context) {
 *         // Implementation
 *     }
 * }
 * }</pre>
 */
public interface TaskTypeHandler {

    /**
     * Returns the task type this handler supports.
     *
     * <p>A handler may support multiple task types by having the registry
     * register it under multiple names.
     *
     * @return the task type string (e.g., "WAIT", "SIMPLE", "HTTP")
     */
    String getTaskType();

    /**
     * Returns the execution mode for the given task.
     *
     * <p>Most handlers return a constant mode, but some (like WAIT) may vary
     * depending on task configuration.
     *
     * @param task the task to determine execution mode for
     * @return the execution mode
     */
    ExecutionMode getExecutionMode(TaskModel task);

    /**
     * Execute the task using the provided context.
     *
     * <p>The context provides access to Temporal primitives (timers, activities,
     * child workflows, signals) and workflow state (DeciderService, SystemTaskExecutor).
     *
     * <p>Implementations should:
     * <ul>
     *   <li>Set task status to IN_PROGRESS if starting execution</li>
     *   <li>Use context methods for async operations (timers, activities, etc.)</li>
     *   <li>Set task status to COMPLETED/FAILED when done (for sync tasks)</li>
     *   <li>Register callbacks for completion (for async tasks)</li>
     * </ul>
     *
     * @param task the task to execute
     * @param workflow the workflow model containing the task
     * @param context the execution context providing access to Temporal primitives
     */
    void execute(TaskModel task, WorkflowModel workflow, TaskExecutionContext context);
}
