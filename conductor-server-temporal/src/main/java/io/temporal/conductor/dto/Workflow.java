package io.temporal.conductor.dto;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Full workflow representation including tasks and definition.
 */
public class Workflow {
    private String workflowId;
    private String parentWorkflowId;
    private String parentWorkflowTaskId;
    private String correlationId;
    private String workflowType;
    private int version;
    private String status;
    private Long endTime;
    private long workflowVersion;
    private WorkflowDef workflowDefinition;
    private List<Task> tasks;
    private Map<String, Object> input;
    private Map<String, Object> output;
    private String externalInputPayloadStoragePath;
    private String externalOutputPayloadStoragePath;
    private Map<String, Object> variables;
    private Long lastRetriedTime;
    private Set<String> failedReferenceTaskNames;
    private String ownerApp;
    private Long createTime;
    private Long updateTime;
    private String createdBy;
    private String updatedBy;
    private String failedTaskId;
    private int priority;
    private String reasonForIncompletion;
    private String event;
    private String taskToDomain;

    public String getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }

    public String getParentWorkflowId() {
        return parentWorkflowId;
    }

    public void setParentWorkflowId(String id) {
        this.parentWorkflowId = id;
    }

    public String getParentWorkflowTaskId() {
        return parentWorkflowTaskId;
    }

    public void setParentWorkflowTaskId(String id) {
        this.parentWorkflowTaskId = id;
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

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public long getWorkflowVersion() {
        return workflowVersion;
    }

    public void setWorkflowVersion(long version) {
        this.workflowVersion = version;
    }

    public WorkflowDef getWorkflowDefinition() {
        return workflowDefinition;
    }

    public void setWorkflowDefinition(WorkflowDef def) {
        this.workflowDefinition = def;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public void setTasks(List<Task> tasks) {
        this.tasks = tasks;
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

    public Map<String, Object> getVariables() {
        return variables;
    }

    public void setVariables(Map<String, Object> variables) {
        this.variables = variables;
    }

    public Long getLastRetriedTime() {
        return lastRetriedTime;
    }

    public void setLastRetriedTime(Long time) {
        this.lastRetriedTime = time;
    }

    public Set<String> getFailedReferenceTaskNames() {
        return failedReferenceTaskNames;
    }

    public void setFailedReferenceTaskNames(Set<String> names) {
        this.failedReferenceTaskNames = names;
    }

    public String getOwnerApp() {
        return ownerApp;
    }

    public void setOwnerApp(String ownerApp) {
        this.ownerApp = ownerApp;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    /**
     * Returns startTime as an alias for createTime (for Conductor UI compatibility).
     * The Conductor OSS Workflow class has this as a computed property.
     */
    public Long getStartTime() {
        return createTime;
    }

    public void setStartTime(Long startTime) {
        this.createTime = startTime;
    }

    public Long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Long updateTime) {
        this.updateTime = updateTime;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }

    public String getFailedTaskId() {
        return failedTaskId;
    }

    public void setFailedTaskId(String failedTaskId) {
        this.failedTaskId = failedTaskId;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public String getReasonForIncompletion() {
        return reasonForIncompletion;
    }

    public void setReasonForIncompletion(String reason) {
        this.reasonForIncompletion = reason;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getTaskToDomain() {
        return taskToDomain;
    }

    public void setTaskToDomain(String taskToDomain) {
        this.taskToDomain = taskToDomain;
    }
}
