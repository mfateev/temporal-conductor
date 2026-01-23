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

/**
 * Task execution log entry.
 */
public class TaskLog {

    private String log;
    private long createdTime;

    /** Default constructor for Jackson. */
    public TaskLog() {
    }

    /**
     * Creates a new task log entry.
     *
     * @param log the log message
     * @param createdTime the timestamp when the log was created
     */
    public TaskLog(String log, long createdTime) {
        this.log = log;
        this.createdTime = createdTime;
    }

    public String getLog() {
        return log;
    }

    public void setLog(String log) {
        this.log = log;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(long createdTime) {
        this.createdTime = createdTime;
    }
}
