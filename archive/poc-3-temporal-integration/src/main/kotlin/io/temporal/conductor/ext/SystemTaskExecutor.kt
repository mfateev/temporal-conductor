package io.temporal.conductor.ext

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.conductor.common.metadata.tasks.TaskType
import com.netflix.conductor.core.config.ConductorProperties
import com.netflix.conductor.core.execution.DeciderService
import com.netflix.conductor.core.execution.tasks.*
import com.netflix.conductor.core.utils.ParametersUtils
import com.netflix.conductor.model.TaskModel
import com.netflix.conductor.model.WorkflowModel
import org.slf4j.LoggerFactory

/**
 * Executes Conductor system tasks using their native implementations.
 *
 * This class delegates ALL system task execution to Conductor's actual system task
 * implementations, keeping the Temporal integration focused on orchestration.
 *
 * Uses in-memory adapters (InMemoryWorkflowExecutor, InMemoryExecutionDAOFacade)
 * to provide required dependencies to system tasks without external database access.
 */
class SystemTaskExecutor(
    deciderService: DeciderService,
    objectMapper: ObjectMapper = ObjectMapper(),
    conductorProperties: ConductorProperties = ConductorProperties()
) {
    private val logger = LoggerFactory.getLogger(SystemTaskExecutor::class.java)

    // In-memory adapters for system task dependencies
    private val inMemoryWorkflowExecutor = InMemoryWorkflowExecutor(deciderService)
    private val inMemoryExecutionDAOFacade = InMemoryExecutionDAOFacade(objectMapper)
    private val parametersUtils = ParametersUtils(objectMapper)

    // Register system tasks using Conductor's native implementations
    private val systemTasks: Map<String, WorkflowSystemTask> = mapOf(
        TaskType.FORK_JOIN.name to Fork(),
        TaskType.JOIN.name to Join(conductorProperties),
        TaskType.SWITCH.name to Switch(),
        TaskType.DECISION.name to Decision(),
        TaskType.TERMINATE.name to Terminate(),
        TaskType.SET_VARIABLE.name to SetVariable(conductorProperties, objectMapper, inMemoryExecutionDAOFacade),
        TaskType.DO_WHILE.name to DoWhile(parametersUtils, inMemoryExecutionDAOFacade)
    )

    private val systemTaskRegistry = SystemTaskRegistry(systemTasks.values.toSet())

    /**
     * Check if a task type is a system task.
     */
    fun isSystemTask(taskType: String): Boolean {
        return systemTaskRegistry.isSystemTask(taskType)
    }

    /**
     * Get the system task implementation for a task type.
     */
    fun getSystemTask(taskType: String): WorkflowSystemTask? {
        return systemTaskRegistry.get(taskType)
    }

    /**
     * Execute a system task.
     *
     * @param workflow The workflow model
     * @param task The task to execute
     * @return true if the task status was changed (task is complete),
     *         false if the task is still in progress (async)
     */
    fun execute(workflow: WorkflowModel, task: TaskModel): Boolean {
        if (!systemTaskRegistry.isSystemTask(task.taskType)) {
            logger.warn("No system task registered for type: {}", task.taskType)
            return false
        }

        val systemTask = systemTaskRegistry.get(task.taskType)

        logger.debug("Executing system task: {} ({})", task.referenceTaskName, task.taskType)

        // Call start() first if task is SCHEDULED
        if (task.status == TaskModel.Status.SCHEDULED) {
            systemTask.start(workflow, task, inMemoryWorkflowExecutor)
            // Set to IN_PROGRESS if start() didn't change status
            if (task.status == TaskModel.Status.SCHEDULED) {
                task.status = TaskModel.Status.IN_PROGRESS
            }
        }

        // Call execute() if task is not terminal
        if (!task.status.isTerminal) {
            val statusChanged = systemTask.execute(workflow, task, inMemoryWorkflowExecutor)
            logger.debug(
                "System task {} execute() returned: {}, status: {}",
                task.referenceTaskName, statusChanged, task.status
            )
            return statusChanged
        }

        return true
    }

    /**
     * Check if a system task is async (needs re-evaluation).
     * Returns false for non-system tasks.
     */
    fun isAsync(taskType: String): Boolean {
        if (!systemTaskRegistry.isSystemTask(taskType)) {
            return false
        }
        return systemTaskRegistry.get(taskType)?.isAsync ?: false
    }

    companion object {
        // Task types that are handled by system tasks
        val SYSTEM_TASK_TYPES = setOf(
            TaskType.FORK_JOIN.name,
            TaskType.JOIN.name,
            TaskType.SWITCH.name,
            TaskType.DECISION.name,
            TaskType.TERMINATE.name,
            TaskType.SET_VARIABLE.name,
            TaskType.DO_WHILE.name,
            TaskType.EXCLUSIVE_JOIN.name
        )

        /**
         * Check if a task type is a known system task type.
         * This includes types that may not be registered but are still system tasks.
         */
        fun isKnownSystemTaskType(taskType: String): Boolean {
            return SYSTEM_TASK_TYPES.contains(taskType)
        }
    }
}
