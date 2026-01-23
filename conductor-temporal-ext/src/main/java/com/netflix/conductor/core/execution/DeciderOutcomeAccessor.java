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

package com.netflix.conductor.core.execution;

import com.netflix.conductor.model.TaskModel;
import java.util.List;

/**
 * Accessor for DeciderOutcome fields which are package-private.
 * This class is in the same package as DeciderService to access the fields.
 */
public final class DeciderOutcomeAccessor {

    private DeciderOutcomeAccessor() {
        // Utility class
    }

    /**
     * Get the tasks to be scheduled from the decider outcome.
     *
     * @param outcome the decider outcome
     * @return list of tasks to be scheduled
     */
    public static List<TaskModel> getTasksToBeScheduled(DeciderService.DeciderOutcome outcome) {
        return outcome.tasksToBeScheduled;
    }

    /**
     * Get the tasks to be updated from the decider outcome.
     *
     * @param outcome the decider outcome
     * @return list of tasks to be updated
     */
    public static List<TaskModel> getTasksToBeUpdated(DeciderService.DeciderOutcome outcome) {
        return outcome.tasksToBeUpdated;
    }

    /**
     * Check if the workflow is complete based on the decider outcome.
     *
     * @param outcome the decider outcome
     * @return true if the workflow is complete
     */
    public static boolean isComplete(DeciderService.DeciderOutcome outcome) {
        return outcome.isComplete;
    }

    /**
     * Get the terminate task from the decider outcome.
     *
     * @param outcome the decider outcome
     * @return the terminate task, or null if not present
     */
    public static TaskModel getTerminateTask(DeciderService.DeciderOutcome outcome) {
        return outcome.terminateTask;
    }
}
