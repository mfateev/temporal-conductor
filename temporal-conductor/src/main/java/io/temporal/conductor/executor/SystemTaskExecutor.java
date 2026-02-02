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

package io.temporal.conductor.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.execution.DeciderService;
import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.EventQueues;
import com.netflix.conductor.core.execution.tasks.Decision;
import com.netflix.conductor.core.execution.tasks.DoWhile;
import com.netflix.conductor.core.execution.tasks.Event;
import com.netflix.conductor.core.execution.tasks.Fork;
import com.netflix.conductor.core.execution.tasks.Human;
import com.netflix.conductor.core.execution.tasks.Join;
import com.netflix.conductor.core.execution.tasks.SetVariable;
import com.netflix.conductor.core.execution.tasks.Switch;
import com.netflix.conductor.core.execution.tasks.SystemTaskRegistry;
import com.netflix.conductor.core.execution.tasks.Terminate;
import com.netflix.conductor.core.execution.tasks.Wait;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.core.utils.ParametersUtils;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes Conductor system tasks using their native implementations.
 *
 * <p>This class delegates ALL system task execution to Conductor's actual system task
 * implementations, keeping the Temporal integration focused on orchestration.
 *
 * <p>Uses in-memory adapters (InMemoryWorkflowExecutor, InMemoryExecutionDAOFacade)
 * to provide required dependencies to system tasks without external database access.
 */
public class SystemTaskExecutor {

    private static final Logger logger = LoggerFactory.getLogger(SystemTaskExecutor.class);

    private static final Set<String> SYSTEM_TASK_TYPES = new HashSet<>();

    static {
        SYSTEM_TASK_TYPES.add(TaskType.FORK_JOIN.name());
        SYSTEM_TASK_TYPES.add(TaskType.TASK_TYPE_FORK); // "FORK" - actual type used in workflows
        SYSTEM_TASK_TYPES.add(TaskType.JOIN.name());
        SYSTEM_TASK_TYPES.add(TaskType.SWITCH.name());
        SYSTEM_TASK_TYPES.add(TaskType.DECISION.name());
        SYSTEM_TASK_TYPES.add(TaskType.TERMINATE.name());
        SYSTEM_TASK_TYPES.add(TaskType.SET_VARIABLE.name());
        SYSTEM_TASK_TYPES.add(TaskType.DO_WHILE.name());
        SYSTEM_TASK_TYPES.add(TaskType.EXCLUSIVE_JOIN.name());
        SYSTEM_TASK_TYPES.add(TaskType.WAIT.name());
        SYSTEM_TASK_TYPES.add(TaskType.SUB_WORKFLOW.name());
        SYSTEM_TASK_TYPES.add(TaskType.START_WORKFLOW.name());
        SYSTEM_TASK_TYPES.add(TaskType.EVENT.name());
        SYSTEM_TASK_TYPES.add(TaskType.HUMAN.name());
    }

    private final InMemoryWorkflowExecutor inMemoryWorkflowExecutor;
    private final InMemoryExecutionDAOFacade inMemoryExecutionDaoFacade;
    private final Map<String, WorkflowSystemTask> systemTasks;
    private final SystemTaskRegistry systemTaskRegistry;

    /**
     * Creates a new SystemTaskExecutor.
     *
     * @param deciderService the DeciderService for task scheduling
     */
    public SystemTaskExecutor(DeciderService deciderService) {
        this(deciderService, new ObjectMapper(), new ConductorProperties());
    }

    /**
     * Creates a new SystemTaskExecutor with custom ObjectMapper.
     *
     * @param deciderService the DeciderService for task scheduling
     * @param objectMapper the ObjectMapper to use
     */
    public SystemTaskExecutor(DeciderService deciderService, ObjectMapper objectMapper) {
        this(deciderService, objectMapper, new ConductorProperties());
    }

    /**
     * Creates a new SystemTaskExecutor with full configuration.
     *
     * @param deciderService the DeciderService for task scheduling
     * @param objectMapper the ObjectMapper to use
     * @param conductorProperties the Conductor properties
     */
    public SystemTaskExecutor(
            DeciderService deciderService,
            ObjectMapper objectMapper,
            ConductorProperties conductorProperties) {

        this.inMemoryWorkflowExecutor = new InMemoryWorkflowExecutor(deciderService);
        this.inMemoryExecutionDaoFacade = new InMemoryExecutionDAOFacade(objectMapper);
        ParametersUtils parametersUtils = new ParametersUtils(objectMapper);

        this.systemTasks = new HashMap<>();
        Fork fork = new Fork();
        systemTasks.put(TaskType.FORK_JOIN.name(), fork);
        systemTasks.put(TaskType.TASK_TYPE_FORK, fork); // "FORK" - actual type used in workflows
        systemTasks.put(TaskType.JOIN.name(), new Join(conductorProperties));
        systemTasks.put(TaskType.SWITCH.name(), new Switch());
        systemTasks.put(TaskType.DECISION.name(), new Decision());
        systemTasks.put(TaskType.TERMINATE.name(), new Terminate());
        systemTasks.put(TaskType.SET_VARIABLE.name(),
                new SetVariable(conductorProperties, objectMapper, inMemoryExecutionDaoFacade));
        systemTasks.put(TaskType.DO_WHILE.name(),
                new DoWhile(parametersUtils, inMemoryExecutionDaoFacade));
        systemTasks.put(TaskType.WAIT.name(), new Wait());
        systemTasks.put(TaskType.HUMAN.name(), new Human());

        this.systemTaskRegistry = new SystemTaskRegistry(new HashSet<>(systemTasks.values()));
    }

    /**
     * Check if a task type is a system task.
     *
     * @param taskType the task type to check
     * @return true if the task type is a system task
     */
    public boolean isSystemTask(String taskType) {
        return systemTaskRegistry.isSystemTask(taskType);
    }

    /**
     * Get the system task implementation for a task type.
     *
     * @param taskType the task type
     * @return the system task implementation, or null if not found
     */
    public WorkflowSystemTask getSystemTask(String taskType) {
        return systemTaskRegistry.get(taskType);
    }

    /**
     * Execute a system task.
     *
     * @param workflow the workflow model
     * @param task the task to execute
     * @return true if the task status was changed (task is complete),
     *         false if the task is still in progress (async)
     */
    public boolean execute(WorkflowModel workflow, TaskModel task) {
        if (!systemTaskRegistry.isSystemTask(task.getTaskType())) {
            logger.warn("No system task registered for type: {}", task.getTaskType());
            return false;
        }

        WorkflowSystemTask systemTask = systemTaskRegistry.get(task.getTaskType());

        if (TaskType.DO_WHILE.name().equals(task.getTaskType())) {
            logger.info("DO_WHILE execute: task={}, status={}, iteration={}, workflowTask={}",
                    task.getReferenceTaskName(), task.getStatus(), task.getIteration(),
                    task.getWorkflowTask() != null ? task.getWorkflowTask().getLoopCondition() : "null");
        } else {
            logger.debug("Executing system task: {} ({})",
                    task.getReferenceTaskName(), task.getTaskType());
        }

        if (task.getStatus() == TaskModel.Status.SCHEDULED) {
            systemTask.start(workflow, task, inMemoryWorkflowExecutor);
            if (task.getStatus() == TaskModel.Status.SCHEDULED) {
                task.setStatus(TaskModel.Status.IN_PROGRESS);
            }
        }

        if (!task.getStatus().isTerminal()) {
            boolean statusChanged = systemTask.execute(workflow, task, inMemoryWorkflowExecutor);

            if (TaskType.DO_WHILE.name().equals(task.getTaskType())) {
                logger.info("DO_WHILE after execute: task={}, statusChanged={}, status={}, iteration={}, outputData={}",
                        task.getReferenceTaskName(), statusChanged, task.getStatus(),
                        task.getIteration(), task.getOutputData());
            } else {
                logger.debug("System task {} execute() returned: {}, status: {}",
                        task.getReferenceTaskName(), statusChanged, task.getStatus());
            }
            return statusChanged;
        }

        return true;
    }

    /**
     * Check if a system task is async (needs re-evaluation).
     * Returns false for non-system tasks.
     *
     * @param taskType the task type to check
     * @return true if the task is async
     */
    public boolean isAsync(String taskType) {
        if (!systemTaskRegistry.isSystemTask(taskType)) {
            return false;
        }
        WorkflowSystemTask task = systemTaskRegistry.get(taskType);
        return task != null && task.isAsync();
    }

    /**
     * Check if a task type is a known system task type.
     * This includes types that may not be registered but are still system tasks.
     *
     * @param taskType the task type to check
     * @return true if the task type is a known system task type
     */
    public static boolean isKnownSystemTaskType(String taskType) {
        return SYSTEM_TASK_TYPES.contains(taskType);
    }
}
