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
 * Activity interface for executing Conductor extension tasks.
 *
 * <p>Extension tasks are Conductor system tasks that come from external libraries
 * (e.g., conductor-kafka, conductor-json-jq-task). These tasks implement
 * {@link com.netflix.conductor.core.execution.tasks.WorkflowSystemTask} and are
 * discovered via Spring's dependency injection.
 *
 * <p>This activity provides a generic execution mechanism for any extension task,
 * allowing them to be executed via Temporal activities with proper durability
 * and retry semantics.
 *
 * <p>Example extension tasks:
 * <ul>
 *   <li>KAFKA_PUBLISH - Publish messages to Kafka</li>
 *   <li>JSON_JQ_TRANSFORM - Transform JSON using jq expressions</li>
 *   <li>HTTP - Advanced HTTP request handling</li>
 * </ul>
 */
@ActivityInterface
public interface ExtensionTaskExecutionActivity {

    /**
     * Execute an extension task.
     *
     * @param taskType the task type (e.g., "KAFKA_PUBLISH", "JSON_JQ_TRANSFORM")
     * @param taskRefName the task reference name for logging and tracking
     * @param inputData the task input data
     * @return the execution result containing status and output data
     */
    @ActivityMethod
    TaskExecutionResult execute(String taskType, String taskRefName, Map<String, Object> inputData);
}
