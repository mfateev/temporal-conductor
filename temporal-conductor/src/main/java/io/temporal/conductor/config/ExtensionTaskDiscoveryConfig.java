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

package io.temporal.conductor.config;

import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import io.temporal.conductor.activity.ExtensionTaskExecutionActivityImpl;
import io.temporal.conductor.handler.TaskTypeHandler;
import io.temporal.conductor.handler.extension.ExtensionTaskAdapter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration for auto-discovering Conductor extension tasks.
 *
 * <p>This configuration:
 * <ol>
 *   <li>Collects all WorkflowSystemTask beans from Spring context</li>
 *   <li>Filters out built-in task types (handled by dedicated handlers)</li>
 *   <li>Creates ExtensionTaskAdapter handlers for discovered extension tasks</li>
 *   <li>Configures the ExtensionTaskExecutionActivityImpl with discovered tasks</li>
 * </ol>
 *
 * <p>To add a new extension task, simply add its dependency to build.gradle.
 * For example, to add KAFKA_PUBLISH support:
 * <pre>{@code
 * implementation 'com.netflix.conductor:conductor-kafka:3.x'
 * }</pre>
 *
 * <p>Spring will automatically discover the KafkaPublishTask bean, and this
 * configuration will create an adapter for it.
 */
@Configuration
public class ExtensionTaskDiscoveryConfig {

    private static final Logger logger = LoggerFactory.getLogger(ExtensionTaskDiscoveryConfig.class);

    /**
     * Built-in task types that have dedicated handlers.
     * These are NOT extension tasks and should not be wrapped with ExtensionTaskAdapter.
     */
    private static final Set<String> BUILT_IN_TASK_TYPES = Set.of(
            TaskType.SIMPLE.name(),
            TaskType.HTTP.name(),
            TaskType.FORK_JOIN.name(),
            TaskType.TASK_TYPE_FORK,
            TaskType.JOIN.name(),
            TaskType.SWITCH.name(),
            TaskType.TERMINATE.name(),
            TaskType.SET_VARIABLE.name(),
            TaskType.DO_WHILE.name(),
            TaskType.EXCLUSIVE_JOIN.name(),
            TaskType.WAIT.name(),
            TaskType.SUB_WORKFLOW.name(),
            TaskType.START_WORKFLOW.name(),
            TaskType.EVENT.name(),
            TaskType.HUMAN.name(),
            "NOOP"
    );

    /**
     * Collect all discovered WorkflowSystemTask beans into a map by task type.
     *
     * @param discoveredTasks optional list of WorkflowSystemTask beans from Spring
     * @return map of task type to WorkflowSystemTask implementation
     */
    @Bean
    public Map<String, WorkflowSystemTask> extensionTasks(
            Optional<List<WorkflowSystemTask>> discoveredTasks) {
        Map<String, WorkflowSystemTask> tasks = new HashMap<>();

        if (discoveredTasks.isPresent()) {
            for (WorkflowSystemTask task : discoveredTasks.get()) {
                String taskType = task.getTaskType();
                // Only include non-built-in tasks
                if (!isBuiltInType(taskType)) {
                    tasks.put(taskType, task);
                    logger.info("Discovered extension task: {}", taskType);
                }
            }
        }

        logger.info("Total extension tasks discovered: {} - {}", tasks.size(), tasks.keySet());
        return tasks;
    }

    /**
     * Create TaskTypeHandler adapters for discovered extension tasks.
     *
     * @param extensionTasks map of discovered extension tasks
     * @return list of TaskTypeHandler adapters for extension tasks
     */
    @Bean
    public List<TaskTypeHandler> extensionHandlers(Map<String, WorkflowSystemTask> extensionTasks) {
        List<TaskTypeHandler> handlers = new ArrayList<>();

        for (String taskType : extensionTasks.keySet()) {
            handlers.add(new ExtensionTaskAdapter(taskType));
            logger.debug("Created extension handler for task type: {}", taskType);
        }

        logger.info("Created {} extension task handlers", handlers.size());
        return handlers;
    }

    /**
     * Create the ExtensionTaskExecutionActivityImpl with discovered extension tasks.
     *
     * @param extensionTasks map of discovered extension tasks
     * @return the activity implementation
     */
    @Bean
    public ExtensionTaskExecutionActivityImpl extensionTaskExecutionActivity(
            Map<String, WorkflowSystemTask> extensionTasks) {
        return new ExtensionTaskExecutionActivityImpl(extensionTasks);
    }

    /**
     * Check if a task type is a built-in type with a dedicated handler.
     *
     * @param taskType the task type to check
     * @return true if this is a built-in type
     */
    private boolean isBuiltInType(String taskType) {
        return BUILT_IN_TASK_TYPES.contains(taskType);
    }
}
