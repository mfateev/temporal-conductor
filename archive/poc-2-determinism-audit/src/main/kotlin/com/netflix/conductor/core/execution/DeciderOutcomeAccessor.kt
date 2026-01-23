package com.netflix.conductor.core.execution

import com.netflix.conductor.model.TaskModel

/**
 * Accessor for DeciderOutcome fields which are package-private.
 * This class is in the same package as DeciderService to access the fields.
 */
object DeciderOutcomeAccessor {
    fun getTasksToBeScheduled(outcome: DeciderService.DeciderOutcome): List<TaskModel> {
        return outcome.tasksToBeScheduled
    }

    fun getTasksToBeUpdated(outcome: DeciderService.DeciderOutcome): List<TaskModel> {
        return outcome.tasksToBeUpdated
    }

    fun isComplete(outcome: DeciderService.DeciderOutcome): Boolean {
        return outcome.isComplete
    }

    fun getTerminateTask(outcome: DeciderService.DeciderOutcome): TaskModel? {
        return outcome.terminateTask
    }
}
