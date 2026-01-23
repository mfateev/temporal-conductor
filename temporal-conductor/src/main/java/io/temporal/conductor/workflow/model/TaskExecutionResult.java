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
 * Result from task execution activity.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskExecutionResult {

    /**
     * Task output data.
     */
    private Map<String, Object> output = Collections.emptyMap();

    /**
     * Task completion status: COMPLETED, FAILED.
     */
    private String status = "COMPLETED";

    /**
     * Failure reason if status is FAILED.
     */
    private String failureReason;

    /** Default constructor for Jackson. */
    public TaskExecutionResult() {
    }

    /**
     * Creates a new TaskExecutionResult with the specified values.
     *
     * @param output the task output data
     * @param status the task status
     * @param failureReason the failure reason if applicable
     */
    public TaskExecutionResult(
            Map<String, Object> output,
            String status,
            String failureReason) {
        this.output = output;
        this.status = status;
        this.failureReason = failureReason;
    }

    // Getters
    public Map<String, Object> getOutput() {
        return output;
    }

    public String getStatus() {
        return status;
    }

    public String getFailureReason() {
        return failureReason;
    }

    // Setters
    public void setOutput(Map<String, Object> output) {
        this.output = output;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    /**
     * Returns a new builder for creating TaskExecutionResult instances.
     *
     * @return a new Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Builder for TaskExecutionResult. */
    public static class Builder {
        private final TaskExecutionResult result = new TaskExecutionResult();

        public Builder output(Map<String, Object> output) {
            result.output = output;
            return this;
        }

        public Builder status(String status) {
            result.status = status;
            return this;
        }

        public Builder failureReason(String failureReason) {
            result.failureReason = failureReason;
            return this;
        }

        public TaskExecutionResult build() {
            return result;
        }
    }
}
