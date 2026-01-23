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

package io.temporal.conductor.activity;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import io.temporal.conductor.workflow.model.TaskExecutionResult;
import java.util.Map;

/**
 * Activity interface for executing Conductor tasks.
 *
 * <p>Each Conductor worker task (SIMPLE, HTTP, etc.) is executed as a Temporal activity,
 * allowing the actual task execution to happen outside the workflow's deterministic context.
 */
@ActivityInterface
public interface TaskExecutionActivities {

    /**
     * Execute a Conductor task.
     *
     * @param taskName The task definition name (e.g., "my_http_task")
     * @param taskRefName The task reference name in this workflow instance
     * @param input The task input data
     * @return Task execution result including output and status
     */
    @ActivityMethod
    TaskExecutionResult executeTask(
            String taskName,
            String taskRefName,
            Map<String, Object> input
    );
}
