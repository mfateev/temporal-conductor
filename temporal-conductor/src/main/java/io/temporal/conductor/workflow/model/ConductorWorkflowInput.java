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

package io.temporal.conductor.workflow.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Input for ConductorWorkflow execution.
 *
 * <p>Contains the workflow definition, input parameters, and task definitions
 * needed to execute a Conductor workflow in Temporal.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConductorWorkflowInput {

    private String workflowDefJson = "";
    private Map<String, Object> workflowInput = Collections.emptyMap();
    private Map<String, String> taskDefsJson = Collections.emptyMap();
    // Additional workflow definitions for SUB_WORKFLOW tasks (keyed by "name:version")
    private Map<String, String> workflowDefsJson = Collections.emptyMap();

    // Metadata for search attributes
    private String correlationId;
    private Integer priority;
    private String ownerApp;
    private String createdBy;
    private List<String> tags;

    // Continue-as-new checkpoint (null for fresh workflows)
    private ContinueAsNewCheckpoint checkpoint;

    /** Default constructor for Jackson. */
    public ConductorWorkflowInput() {
    }

    // Getters
    public String getWorkflowDefJson() {
        return workflowDefJson;
    }

    public Map<String, Object> getWorkflowInput() {
        return workflowInput;
    }

    public Map<String, String> getTaskDefsJson() {
        return taskDefsJson;
    }

    public Map<String, String> getWorkflowDefsJson() {
        return workflowDefsJson;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public Integer getPriority() {
        return priority;
    }

    public String getOwnerApp() {
        return ownerApp;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public List<String> getTags() {
        return tags;
    }

    public ContinueAsNewCheckpoint getCheckpoint() {
        return checkpoint;
    }

    // Setters
    public void setWorkflowDefJson(String workflowDefJson) {
        this.workflowDefJson = workflowDefJson;
    }

    public void setWorkflowInput(Map<String, Object> workflowInput) {
        this.workflowInput = workflowInput;
    }

    public void setTaskDefsJson(Map<String, String> taskDefsJson) {
        this.taskDefsJson = taskDefsJson;
    }

    public void setWorkflowDefsJson(Map<String, String> workflowDefsJson) {
        this.workflowDefsJson = workflowDefsJson;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public void setOwnerApp(String ownerApp) {
        this.ownerApp = ownerApp;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public void setCheckpoint(ContinueAsNewCheckpoint checkpoint) {
        this.checkpoint = checkpoint;
    }

    /**
     * Returns a new builder for creating ConductorWorkflowInput instances.
     *
     * @return a new Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for ConductorWorkflowInput. */
    public static class Builder {
        private final ConductorWorkflowInput input = new ConductorWorkflowInput();

        public Builder workflowDefJson(String json) {
            input.workflowDefJson = json;
            return this;
        }

        public Builder workflowInput(Map<String, Object> workflowInput) {
            input.workflowInput = workflowInput;
            return this;
        }

        public Builder taskDefsJson(Map<String, String> taskDefsJson) {
            input.taskDefsJson = taskDefsJson;
            return this;
        }

        public Builder workflowDefsJson(Map<String, String> workflowDefsJson) {
            input.workflowDefsJson = workflowDefsJson;
            return this;
        }

        public Builder correlationId(String correlationId) {
            input.correlationId = correlationId;
            return this;
        }

        public Builder priority(Integer priority) {
            input.priority = priority;
            return this;
        }

        public Builder ownerApp(String ownerApp) {
            input.ownerApp = ownerApp;
            return this;
        }

        public Builder createdBy(String createdBy) {
            input.createdBy = createdBy;
            return this;
        }

        public Builder tags(List<String> tags) {
            input.tags = tags;
            return this;
        }

        public Builder checkpoint(ContinueAsNewCheckpoint checkpoint) {
            input.checkpoint = checkpoint;
            return this;
        }

        public ConductorWorkflowInput build() {
            return input;
        }
    }
}
