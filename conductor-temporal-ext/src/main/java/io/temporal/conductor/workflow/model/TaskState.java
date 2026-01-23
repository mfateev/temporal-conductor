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
 * Task state for UI display.
 * Maps to Conductor's Task object structure.
 */
public class TaskState {

    // Identity
    private String taskId;
    private String taskType;
    private String taskDefName;
    private String referenceTaskName;

    // Status
    private String status;
    private String reasonForIncompletion;
    private int retryCount;

    // Timing
    private long scheduledTime;
    private long startTime;
    private long updateTime;
    private long endTime;

    // Data
    private Map<String, Object> inputData;
    private Map<String, Object> outputData;

    // Execution info
    private String workerId;
    private int pollCount;
    private int iteration;

    // Logs
    private List<TaskLog> logs;

    /** Default constructor for Jackson. */
    public TaskState() {
    }

    /**
     * Returns a new builder for creating TaskState instances.
     *
     * @return a new Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for TaskState. */
    public static class Builder {
        private final TaskState state = new TaskState();

        public Builder taskId(String taskId) {
            state.taskId = taskId;
            return this;
        }

        public Builder taskType(String taskType) {
            state.taskType = taskType;
            return this;
        }

        public Builder taskDefName(String taskDefName) {
            state.taskDefName = taskDefName;
            return this;
        }

        public Builder referenceTaskName(String referenceTaskName) {
            state.referenceTaskName = referenceTaskName;
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

        public Builder retryCount(int retryCount) {
            state.retryCount = retryCount;
            return this;
        }

        public Builder scheduledTime(long scheduledTime) {
            state.scheduledTime = scheduledTime;
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

        public Builder endTime(long endTime) {
            state.endTime = endTime;
            return this;
        }

        public Builder inputData(Map<String, Object> inputData) {
            state.inputData = inputData;
            return this;
        }

        public Builder outputData(Map<String, Object> outputData) {
            state.outputData = outputData;
            return this;
        }

        public Builder workerId(String workerId) {
            state.workerId = workerId;
            return this;
        }

        public Builder pollCount(int pollCount) {
            state.pollCount = pollCount;
            return this;
        }

        public Builder iteration(int iteration) {
            state.iteration = iteration;
            return this;
        }

        public Builder logs(List<TaskLog> logs) {
            state.logs = logs;
            return this;
        }

        public TaskState build() {
            return state;
        }
    }

    // Getters
    public String getTaskId() {
        return taskId;
    }

    public String getTaskType() {
        return taskType;
    }

    public String getTaskDefName() {
        return taskDefName;
    }

    public String getReferenceTaskName() {
        return referenceTaskName;
    }

    public String getStatus() {
        return status;
    }

    public String getReasonForIncompletion() {
        return reasonForIncompletion;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public long getScheduledTime() {
        return scheduledTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public Map<String, Object> getInputData() {
        return inputData;
    }

    public Map<String, Object> getOutputData() {
        return outputData;
    }

    public String getWorkerId() {
        return workerId;
    }

    public int getPollCount() {
        return pollCount;
    }

    public int getIteration() {
        return iteration;
    }

    public List<TaskLog> getLogs() {
        return logs;
    }

    // Setters
    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public void setTaskDefName(String taskDefName) {
        this.taskDefName = taskDefName;
    }

    public void setReferenceTaskName(String referenceTaskName) {
        this.referenceTaskName = referenceTaskName;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setReasonForIncompletion(String reason) {
        this.reasonForIncompletion = reason;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public void setScheduledTime(long scheduledTime) {
        this.scheduledTime = scheduledTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setUpdateTime(long updateTime) {
        this.updateTime = updateTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public void setInputData(Map<String, Object> inputData) {
        this.inputData = inputData;
    }

    public void setOutputData(Map<String, Object> outputData) {
        this.outputData = outputData;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public void setPollCount(int pollCount) {
        this.pollCount = pollCount;
    }

    public void setIteration(int iteration) {
        this.iteration = iteration;
    }

    public void setLogs(List<TaskLog> logs) {
        this.logs = logs;
    }
}
