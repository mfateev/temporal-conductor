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

package io.temporal.conductor.e2e.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Response DTO for workflow status from Conductor REST API.
 * Maps the essential fields needed for E2E test verification.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkflowStatusResponse {

    private String workflowId;
    private String correlationId;
    private String workflowType;
    private int version;
    private String status;
    private Long startTime;
    private Long endTime;
    private Map<String, Object> input;
    private Map<String, Object> output;
    private List<TaskInfo> tasks;
    private Set<String> failedReferenceTaskNames;
    private String reasonForIncompletion;
    private int priority;
    private String ownerApp;

    public String getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public String getWorkflowType() {
        return workflowType;
    }

    public void setWorkflowType(String workflowType) {
        this.workflowType = workflowType;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public Map<String, Object> getInput() {
        return input;
    }

    public void setInput(Map<String, Object> input) {
        this.input = input;
    }

    public Map<String, Object> getOutput() {
        return output;
    }

    public void setOutput(Map<String, Object> output) {
        this.output = output;
    }

    public List<TaskInfo> getTasks() {
        return tasks;
    }

    public void setTasks(List<TaskInfo> tasks) {
        this.tasks = tasks;
    }

    public Set<String> getFailedReferenceTaskNames() {
        return failedReferenceTaskNames;
    }

    public void setFailedReferenceTaskNames(Set<String> failedReferenceTaskNames) {
        this.failedReferenceTaskNames = failedReferenceTaskNames;
    }

    public String getReasonForIncompletion() {
        return reasonForIncompletion;
    }

    public void setReasonForIncompletion(String reasonForIncompletion) {
        this.reasonForIncompletion = reasonForIncompletion;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public String getOwnerApp() {
        return ownerApp;
    }

    public void setOwnerApp(String ownerApp) {
        this.ownerApp = ownerApp;
    }

    /**
     * Task information within workflow status.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TaskInfo {
        private String taskId;
        private String taskType;
        private String taskDefName;
        private String referenceTaskName;
        private String status;
        private Map<String, Object> inputData;
        private Map<String, Object> outputData;
        private Long startTime;
        private Long endTime;

        public String getTaskId() {
            return taskId;
        }

        public void setTaskId(String taskId) {
            this.taskId = taskId;
        }

        public String getTaskType() {
            return taskType;
        }

        public void setTaskType(String taskType) {
            this.taskType = taskType;
        }

        public String getTaskDefName() {
            return taskDefName;
        }

        public void setTaskDefName(String taskDefName) {
            this.taskDefName = taskDefName;
        }

        public String getReferenceTaskName() {
            return referenceTaskName;
        }

        public void setReferenceTaskName(String referenceTaskName) {
            this.referenceTaskName = referenceTaskName;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public Map<String, Object> getInputData() {
            return inputData;
        }

        public void setInputData(Map<String, Object> inputData) {
            this.inputData = inputData;
        }

        public Map<String, Object> getOutputData() {
            return outputData;
        }

        public void setOutputData(Map<String, Object> outputData) {
            this.outputData = outputData;
        }

        public Long getStartTime() {
            return startTime;
        }

        public void setStartTime(Long startTime) {
            this.startTime = startTime;
        }

        public Long getEndTime() {
            return endTime;
        }

        public void setEndTime(Long endTime) {
            this.endTime = endTime;
        }
    }
}
