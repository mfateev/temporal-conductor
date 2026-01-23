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

import io.temporal.common.SearchAttributeKey;

/**
 * Search attribute keys for Conductor definition workflows.
 *
 * <p>These attributes enable visibility queries for listing and filtering workflow and task
 * definitions stored as Temporal workflows.
 *
 * <p>Registration script:
 * <pre>
 * temporal operator search-attribute create --namespace conductor \
 *   --name ConductorDefinitionType --type Keyword
 * temporal operator search-attribute create --namespace conductor \
 *   --name ConductorDefinitionName --type Keyword
 * temporal operator search-attribute create --namespace conductor \
 *   --name ConductorDefinitionVersion --type Int
 * </pre>
 */
public final class DefinitionSearchAttributes {

    /** Definition type: "workflow" or "task". */
    public static final SearchAttributeKey<String> DEFINITION_TYPE =
            SearchAttributeKey.forKeyword("ConductorDefinitionType");

    /** Definition name (workflow name or task type). */
    public static final SearchAttributeKey<String> DEFINITION_NAME =
            SearchAttributeKey.forKeyword("ConductorDefinitionName");

    /** Definition version (only applicable to workflow definitions). */
    public static final SearchAttributeKey<Long> DEFINITION_VERSION =
            SearchAttributeKey.forLong("ConductorDefinitionVersion");

    /** WorkflowId prefix for definition workflows. */
    public static final String WORKFLOW_ID_PREFIX = "conductor-def:";

    /** WorkflowId prefix for workflow definitions. */
    public static final String WORKFLOW_DEF_PREFIX = WORKFLOW_ID_PREFIX + "workflow:";

    /** WorkflowId prefix for task definitions. */
    public static final String TASK_DEF_PREFIX = WORKFLOW_ID_PREFIX + "task:";

    private DefinitionSearchAttributes() {
        // Prevent instantiation
    }

    /**
     * Generate a workflow ID for a workflow definition.
     *
     * @param name workflow name
     * @param version workflow version
     * @return workflow ID in format: conductor-def:workflow:{name}:{version}
     */
    public static String workflowDefId(String name, int version) {
        return WORKFLOW_DEF_PREFIX + name + ":" + version;
    }

    /**
     * Generate a workflow ID for a task definition.
     *
     * @param taskType task type name
     * @return workflow ID in format: conductor-def:task:{taskType}
     */
    public static String taskDefId(String taskType) {
        return TASK_DEF_PREFIX + taskType;
    }
}
