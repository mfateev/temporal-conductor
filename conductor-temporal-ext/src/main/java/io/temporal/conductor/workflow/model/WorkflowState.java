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

import java.util.List;
import java.util.Map;

/**
 * Full workflow state for UI display.
 * Maps to Conductor's Workflow object structure.
 */
public class WorkflowState {

    // Identity
    private String workflowId;
    private String workflowType;
    private int version;

    // Status
    private String status;
    private String reasonForIncompletion;

    // Timing
    private long createTime;
    private long startTime;
    private long updateTime;
    private Long endTime;

    // Data
    private Map<String, Object> input;
    private Map<String, Object> output;
    private Map<String, Object> variables;

    // Metadata
    private String correlationId;
    private int priority;
    private String ownerApp;
    private String createdBy;

    // Tasks
    private List<TaskState> tasks;

    /** Default constructor for Jackson. */
    public WorkflowState() {
    }

    /**
     * Returns a new builder for creating WorkflowState instances.
     *
     * @return a new Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for WorkflowState. */
    public static class Builder {
        private final WorkflowState state = new WorkflowState();

        public Builder workflowId(String workflowId) {
            state.workflowId = workflowId;
            return this;
        }

        public Builder workflowType(String workflowType) {
            state.workflowType = workflowType;
            return this;
        }

        public Builder version(int version) {
            state.version = version;
            return this;
        }

        public Builder status(String status) {
            state.status = status;
            return this;
        }

        public Builder reasonForIncompletion(String reason) {
            state.reasonForIncompletion = reason;
            return this;
        }

        public Builder createTime(long createTime) {
            state.createTime = createTime;
            return this;
        }

        public Builder startTime(long startTime) {
            state.startTime = startTime;
            return this;
        }

        public Builder updateTime(long updateTime) {
            state.updateTime = updateTime;
            return this;
        }

        public Builder endTime(Long endTime) {
            state.endTime = endTime;
            return this;
        }

        public Builder input(Map<String, Object> input) {
            state.input = input;
            return this;
        }

        public Builder output(Map<String, Object> output) {
            state.output = output;
            return this;
        }

        public Builder variables(Map<String, Object> variables) {
            state.variables = variables;
            return this;
        }

        public Builder correlationId(String correlationId) {
            state.correlationId = correlationId;
            return this;
        }

        public Builder priority(int priority) {
            state.priority = priority;
            return this;
        }

        public Builder ownerApp(String ownerApp) {
            state.ownerApp = ownerApp;
            return this;
        }

        public Builder createdBy(String createdBy) {
            state.createdBy = createdBy;
            return this;
        }

        public Builder tasks(List<TaskState> tasks) {
            state.tasks = tasks;
            return this;
        }

        public WorkflowState build() {
            return state;
        }
    }

    // Getters
    public String getWorkflowId() {
        return workflowId;
    }

    public String getWorkflowType() {
        return workflowType;
    }

    public int getVersion() {
        return version;
    }

    public String getStatus() {
        return status;
    }

    public String getReasonForIncompletion() {
        return reasonForIncompletion;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public Map<String, Object> getInput() {
        return input;
    }

    public Map<String, Object> getOutput() {
        return output;
    }

    public Map<String, Object> getVariables() {
        return variables;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public int getPriority() {
        return priority;
    }

    public String getOwnerApp() {
        return ownerApp;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public List<TaskState> getTasks() {
        return tasks;
    }

    // Setters
    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }

    public void setWorkflowType(String workflowType) {
        this.workflowType = workflowType;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setReasonForIncompletion(String reason) {
        this.reasonForIncompletion = reason;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public void setInput(Map<String, Object> input) {
        this.input = input;
    }

    public void setOutput(Map<String, Object> output) {
        this.output = output;
    }

    public void setVariables(Map<String, Object> variables) {
        this.variables = variables;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public void setOwnerApp(String ownerApp) {
        this.ownerApp = ownerApp;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public void setTasks(List<TaskState> tasks) {
        this.tasks = tasks;
    }
}
