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
import java.util.Map;

/**
 * Output from ConductorWorkflow execution.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConductorWorkflowOutput {

    /**
     * Final workflow status: COMPLETED, FAILED, TIMED_OUT, TERMINATED.
     */
    private String status = "";

    /**
     * Workflow output data from the last task or explicit output.
     */
    private Map<String, Object> output = Collections.emptyMap();

    /**
     * Reason for failure if status is FAILED or TIMED_OUT.
     */
    private String failureReason;

    /**
     * Task outputs keyed by reference name.
     * Useful for testing to verify which tasks executed.
     */
    private Map<String, Map<String, Object>> taskOutputs = Collections.emptyMap();

    /** Default constructor for Jackson. */
    public ConductorWorkflowOutput() {
    }

    /**
     * Creates a new ConductorWorkflowOutput with the specified values.
     *
     * @param status the workflow status
     * @param output the workflow output data
     * @param failureReason the failure reason if applicable
     * @param taskOutputs the task outputs keyed by reference name
     */
    public ConductorWorkflowOutput(
            String status,
            Map<String, Object> output,
            String failureReason,
            Map<String, Map<String, Object>> taskOutputs) {
        this.status = status;
        this.output = output;
        this.failureReason = failureReason;
        this.taskOutputs = taskOutputs;
    }

    // Getters
    public String getStatus() {
        return status;
    }

    public Map<String, Object> getOutput() {
        return output;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public Map<String, Map<String, Object>> getTaskOutputs() {
        return taskOutputs;
    }

    // Setters
    public void setStatus(String status) {
        this.status = status;
    }

    public void setOutput(Map<String, Object> output) {
        this.output = output;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    public void setTaskOutputs(Map<String, Map<String, Object>> taskOutputs) {
        this.taskOutputs = taskOutputs;
    }

    /**
     * Returns a new builder for creating ConductorWorkflowOutput instances.
     *
     * @return a new Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for ConductorWorkflowOutput. */
    public static class Builder {
        private final ConductorWorkflowOutput output = new ConductorWorkflowOutput();

        public Builder status(String status) {
            output.status = status;
            return this;
        }

        public Builder output(Map<String, Object> outputData) {
            output.output = outputData;
            return this;
        }

        public Builder failureReason(String failureReason) {
            output.failureReason = failureReason;
            return this;
        }

        public Builder taskOutputs(Map<String, Map<String, Object>> taskOutputs) {
            output.taskOutputs = taskOutputs;
            return this;
        }

        public ConductorWorkflowOutput build() {
            return output;
        }
    }
}
