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

import com.netflix.conductor.common.metadata.tasks.TaskType;
import io.temporal.conductor.handler.impl.AsyncSystemTaskHandler;
import io.temporal.conductor.handler.impl.EventTaskHandler;
import io.temporal.conductor.handler.impl.HumanTaskHandler;
import io.temporal.conductor.handler.impl.StartWorkflowTaskHandler;
import io.temporal.conductor.handler.impl.SubWorkflowTaskHandler;
import io.temporal.conductor.handler.impl.SyncSystemTaskHandler;
import io.temporal.conductor.handler.impl.WaitTaskHandler;
import io.temporal.conductor.handler.impl.WorkerTaskHandler;
import java.util.List;

/**
 * Factory for creating TaskTypeHandlerRegistry with all built-in handlers.
 *
 * <p>This factory creates and registers all the standard task type handlers:
 * <ul>
 *   <li>Sync system tasks (FORK, JOIN, SWITCH, SET_VARIABLE, TERMINATE)</li>
 *   <li>Async system tasks (DO_WHILE, JOIN, EXCLUSIVE_JOIN)</li>
 *   <li>WAIT tasks (timer and signal-based)</li>
 *   <li>HUMAN tasks (signal-based completion)</li>
 *   <li>SUB_WORKFLOW tasks (child workflow with wait)</li>
 *   <li>START_WORKFLOW tasks (fire-and-forget child workflow)</li>
 *   <li>EVENT tasks (queue publishing)</li>
 *   <li>Worker tasks (SIMPLE, HTTP, custom - default handler)</li>
 * </ul>
 */
public class TaskTypeHandlerFactory {

    /**
     * Create a fully configured handler registry with all built-in handlers.
     *
     * @return the configured registry
     */
    public static TaskTypeHandlerRegistry createRegistry() {
        // Default handler for unknown task types (treated as worker tasks)
        WorkerTaskHandler defaultHandler = new WorkerTaskHandler();

        TaskTypeHandlerRegistry registry = new TaskTypeHandlerRegistry(defaultHandler);

        // Register sync system task handlers
        for (String taskType : SyncSystemTaskHandler.SUPPORTED_TYPES) {
            registry.register(taskType, new SyncSystemTaskHandler(taskType));
        }

        // Register async system task handlers (need re-evaluation)
        for (String taskType : AsyncSystemTaskHandler.SUPPORTED_TYPES) {
            registry.register(taskType, new AsyncSystemTaskHandler(taskType));
        }

        // Register special task handlers
        registry.register(new WaitTaskHandler());
        registry.register(new HumanTaskHandler());
        registry.register(new SubWorkflowTaskHandler());
        registry.register(new StartWorkflowTaskHandler());
        registry.register(new EventTaskHandler());

        // Register known worker task types explicitly
        registry.register(TaskType.SIMPLE.name(), new WorkerTaskHandler(TaskType.SIMPLE.name()));
        registry.register(TaskType.HTTP.name(), new WorkerTaskHandler(TaskType.HTTP.name()));

        return registry;
    }

    /**
     * Create a handler registry with additional custom handlers.
     *
     * @param additionalHandlers additional handlers to register
     * @return the configured registry
     */
    public static TaskTypeHandlerRegistry createRegistry(List<TaskTypeHandler> additionalHandlers) {
        TaskTypeHandlerRegistry registry = createRegistry();

        for (TaskTypeHandler handler : additionalHandlers) {
            registry.register(handler);
        }

        return registry;
    }
}
