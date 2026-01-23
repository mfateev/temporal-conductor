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

package io.temporal.conductor.workflow;

import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

/**
 * Temporal workflow interface for storing Conductor definitions.
 *
 * <p>Each workflow/task definition becomes a long-running Temporal workflow that holds state
 * indefinitely. This allows metadata to persist across server restarts using Temporal's durable
 * execution.
 *
 * <p>WorkflowId patterns:
 * <ul>
 *   <li>Workflow definitions: {@code conductor-def:workflow:{name}:{version}}</li>
 *   <li>Task definitions: {@code conductor-def:task:{name}}</li>
 * </ul>
 */
@WorkflowInterface
public interface DefinitionWorkflow {

    /**
     * Main workflow method that holds state forever.
     *
     * <p>The workflow waits indefinitely using {@code Workflow.await(() -> false)}, keeping the
     * definition stored in Temporal's event history. The definition can be retrieved via query and
     * updated via signal.
     */
    @WorkflowMethod
    void hold();

    /**
     * Query to retrieve the definition as JSON.
     *
     * @return The definition serialized as JSON, or null if not yet set
     */
    @QueryMethod
    String getDefinitionJson();

    /**
     * Signal to update the definition.
     *
     * @param definitionJson The new definition serialized as JSON
     */
    @SignalMethod
    void updateDefinition(String definitionJson);
}
