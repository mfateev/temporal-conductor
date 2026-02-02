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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registry for task type handlers.
 *
 * <p>The registry maintains a mapping from task type names to their handlers.
 * When a task type is not found, the default handler (typically {@code WorkerTaskHandler})
 * is returned for handling as a worker task.
 *
 * <p>Handlers can be registered at construction time or added dynamically
 * via {@link #register(TaskTypeHandler)} or {@link #register(String, TaskTypeHandler)}.
 *
 * <p>Usage:
 * <pre>{@code
 * TaskTypeHandlerRegistry registry = new TaskTypeHandlerRegistry(
 *     workerTaskHandler,
 *     List.of(
 *         new SyncSystemTaskHandler(),
 *         new WaitTaskHandler(),
 *         new HumanTaskHandler()
 *     )
 * );
 *
 * // Get handler for a task type
 * TaskTypeHandler handler = registry.getHandler("WAIT");
 * handler.execute(task, workflow, context);
 * }</pre>
 */
public class TaskTypeHandlerRegistry {

    private static final Logger logger = LoggerFactory.getLogger(TaskTypeHandlerRegistry.class);

    private final Map<String, TaskTypeHandler> handlers = new HashMap<>();
    private final TaskTypeHandler defaultHandler;

    /**
     * Creates a registry with a default handler.
     *
     * @param defaultHandler the handler to use for unknown task types
     */
    public TaskTypeHandlerRegistry(TaskTypeHandler defaultHandler) {
        this.defaultHandler = defaultHandler;
    }

    /**
     * Creates a registry with a default handler and initial handlers.
     *
     * @param defaultHandler the handler to use for unknown task types
     * @param handlers the initial handlers to register
     */
    public TaskTypeHandlerRegistry(TaskTypeHandler defaultHandler, Collection<TaskTypeHandler> handlers) {
        this.defaultHandler = defaultHandler;
        for (TaskTypeHandler handler : handlers) {
            register(handler);
        }
    }

    /**
     * Register a handler for its declared task type.
     *
     * @param handler the handler to register
     */
    public void register(TaskTypeHandler handler) {
        register(handler.getTaskType(), handler);
    }

    /**
     * Register a handler under a specific task type name.
     *
     * <p>This allows a handler to be registered under multiple names
     * (e.g., "FORK" and "FORK_JOIN" for the same handler).
     *
     * @param taskType the task type name
     * @param handler the handler
     */
    public void register(String taskType, TaskTypeHandler handler) {
        TaskTypeHandler existing = handlers.put(taskType, handler);
        if (existing != null) {
            logger.warn("Overwriting handler for task type '{}': {} -> {}",
                    taskType, existing.getClass().getSimpleName(), handler.getClass().getSimpleName());
        } else {
            logger.debug("Registered handler for task type '{}': {}",
                    taskType, handler.getClass().getSimpleName());
        }
    }

    /**
     * Get the handler for a task type.
     *
     * <p>If no handler is registered for the task type, the default handler
     * is returned (typically treating the task as a worker task).
     *
     * @param taskType the task type name
     * @return the handler for this task type, or the default handler
     */
    public TaskTypeHandler getHandler(String taskType) {
        return handlers.getOrDefault(taskType, defaultHandler);
    }

    /**
     * Check if a handler is registered for a task type.
     *
     * @param taskType the task type name
     * @return true if a specific handler is registered (not using default)
     */
    public boolean hasHandler(String taskType) {
        return handlers.containsKey(taskType);
    }

    /**
     * Get all registered task types.
     *
     * @return unmodifiable set of registered task type names
     */
    public Set<String> getRegisteredTaskTypes() {
        return Collections.unmodifiableSet(handlers.keySet());
    }

    /**
     * Get the default handler.
     *
     * @return the default handler used for unknown task types
     */
    public TaskTypeHandler getDefaultHandler() {
        return defaultHandler;
    }
}
