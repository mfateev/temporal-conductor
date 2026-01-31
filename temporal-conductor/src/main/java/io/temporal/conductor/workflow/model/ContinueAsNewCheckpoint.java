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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Checkpoint state for continue-as-new operations.
 *
 * <p>This class captures the workflow state needed to resume execution
 * after a continue-as-new operation. It includes:
 * <ul>
 *   <li>Completed task history and outputs</li>
 *   <li>Workflow variables</li>
 *   <li>Metadata (correlation ID, priority, etc.)</li>
 *   <li>ID generator sequence for deterministic task ID generation</li>
 *   <li>Pending timers to recreate</li>
 *   <li>Pending signals to process</li>
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ContinueAsNewCheckpoint {

    // Task state
    private List<TaskSnapshot> completedTasks = new ArrayList<>();
    private Map<String, Object> variables = new HashMap<>();
    private Map<String, Map<String, Object>> taskOutputs = new HashMap<>();

    // Metadata
    private String correlationId;
    private int priority;
    private String ownerApp;
    private String createdBy;
    private long originalCreateTime;

    // ID generator state
    private long lastTaskIdSequence;

    // Pending state to recreate
    private List<PendingTimer> pendingTimers = new ArrayList<>();
    private Map<String, Map<String, Object>> pendingSignals = new HashMap<>();

    // Tracking
    private int continueAsNewCount;

    /** Default constructor for Jackson. */
    public ContinueAsNewCheckpoint() {
    }

    // Getters and setters

    public List<TaskSnapshot> getCompletedTasks() {
        return completedTasks;
    }

    public void setCompletedTasks(List<TaskSnapshot> completedTasks) {
        this.completedTasks = completedTasks;
    }

    public Map<String, Object> getVariables() {
        return variables;
    }

    public void setVariables(Map<String, Object> variables) {
        this.variables = variables;
    }

    public Map<String, Map<String, Object>> getTaskOutputs() {
        return taskOutputs;
    }

    public void setTaskOutputs(Map<String, Map<String, Object>> taskOutputs) {
        this.taskOutputs = taskOutputs;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
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

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public long getOriginalCreateTime() {
        return originalCreateTime;
    }

    public void setOriginalCreateTime(long originalCreateTime) {
        this.originalCreateTime = originalCreateTime;
    }

    public long getLastTaskIdSequence() {
        return lastTaskIdSequence;
    }

    public void setLastTaskIdSequence(long lastTaskIdSequence) {
        this.lastTaskIdSequence = lastTaskIdSequence;
    }

    public List<PendingTimer> getPendingTimers() {
        return pendingTimers;
    }

    public void setPendingTimers(List<PendingTimer> pendingTimers) {
        this.pendingTimers = pendingTimers;
    }

    public Map<String, Map<String, Object>> getPendingSignals() {
        return pendingSignals;
    }

    public void setPendingSignals(Map<String, Map<String, Object>> pendingSignals) {
        this.pendingSignals = pendingSignals;
    }

    public int getContinueAsNewCount() {
        return continueAsNewCount;
    }

    public void setContinueAsNewCount(int continueAsNewCount) {
        this.continueAsNewCount = continueAsNewCount;
    }
}
