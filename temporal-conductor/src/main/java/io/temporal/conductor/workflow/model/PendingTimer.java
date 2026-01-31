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

/**
 * Represents a pending WAIT task timer that needs to be recreated after continue-as-new.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PendingTimer {

    private String taskRefName;
    private long remainingDurationMs;

    /** Default constructor for Jackson. */
    public PendingTimer() {
    }

    public PendingTimer(String taskRefName, long remainingDurationMs) {
        this.taskRefName = taskRefName;
        this.remainingDurationMs = remainingDurationMs;
    }

    public String getTaskRefName() {
        return taskRefName;
    }

    public void setTaskRefName(String taskRefName) {
        this.taskRefName = taskRefName;
    }

    public long getRemainingDurationMs() {
        return remainingDurationMs;
    }

    public void setRemainingDurationMs(long remainingDurationMs) {
        this.remainingDurationMs = remainingDurationMs;
    }
}
