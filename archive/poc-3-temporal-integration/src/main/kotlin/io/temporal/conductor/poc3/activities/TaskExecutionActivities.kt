package io.temporal.conductor.poc3.activities

import io.temporal.activity.ActivityInterface
import io.temporal.activity.ActivityMethod
import io.temporal.conductor.poc3.model.TaskExecutionResult

/**
 * Activity interface for executing Conductor tasks.
 *
 * Each Conductor worker task (SIMPLE, HTTP, etc.) is executed as a Temporal activity,
 * allowing the actual task execution to happen outside the workflow's deterministic context.
 */
@ActivityInterface
interface TaskExecutionActivities {
    /**
     * Execute a Conductor task.
     *
     * @param taskName The task definition name (e.g., "my_http_task")
     * @param taskRefName The task reference name in this workflow instance
     * @param input The task input data
     * @return Task execution result including output and status
     */
    @ActivityMethod
    fun executeTask(
        taskName: String,
        taskRefName: String,
        input: Map<String, Any>
    ): TaskExecutionResult
}
