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

import io.temporal.workflow.Workflow;

/**
 * Implementation of the definition workflow that stores metadata as durable state.
 *
 * <p>This workflow holds a definition (workflow or task) as JSON and keeps it persisted via
 * Temporal's event history. The workflow runs forever using {@code Workflow.await(() -> false)},
 * ensuring the definition survives server restarts.
 */
public class DefinitionWorkflowImpl implements DefinitionWorkflow {

    private String definitionJson;

    @Override
    public void hold() {
        // Wait forever - workflow just holds state
        // The definition is stored in Temporal's event history and survives restarts
        Workflow.await(() -> false);
    }

    @Override
    public String getDefinitionJson() {
        return definitionJson;
    }

    @Override
    public void updateDefinition(String definitionJson) {
        this.definitionJson = definitionJson;
    }
}
