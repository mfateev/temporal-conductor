package io.temporal.conductor.dto;

import java.util.Set;

/**
 * Summary view of a workflow execution, used in search results.
 */
public class WorkflowSummary {
    private String workflowType;
    private int version;
    private String workflowId;
    private String correlationId;
    private Long startTime;
    private Long updateTime;
    private Long endTime;
    private String status;
    private String input;
    private String output;
    private String reasonForIncompletion;
    private long executionTime;
    private String event;
    private String failedReferenceTaskNames;
    private String externalInputPayloadStoragePath;
    private String externalOutputPayloadStoragePath;
    private int priority;
    private Set<String> failedTaskNames;

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

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Long updateTime) {
        this.updateTime = updateTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public String getReasonForIncompletion() {
        return reasonForIncompletion;
    }

    public void setReasonForIncompletion(String reason) {
        this.reasonForIncompletion = reason;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(long executionTime) {
        this.executionTime = executionTime;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getFailedReferenceTaskNames() {
        return failedReferenceTaskNames;
    }

    public void setFailedReferenceTaskNames(String names) {
        this.failedReferenceTaskNames = names;
    }

    public String getExternalInputPayloadStoragePath() {
        return externalInputPayloadStoragePath;
    }

    public void setExternalInputPayloadStoragePath(String path) {
        this.externalInputPayloadStoragePath = path;
    }

    public String getExternalOutputPayloadStoragePath() {
        return externalOutputPayloadStoragePath;
    }

    public void setExternalOutputPayloadStoragePath(String path) {
        this.externalOutputPayloadStoragePath = path;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public Set<String> getFailedTaskNames() {
        return failedTaskNames;
    }

    public void setFailedTaskNames(Set<String> failedTaskNames) {
        this.failedTaskNames = failedTaskNames;
    }
}
