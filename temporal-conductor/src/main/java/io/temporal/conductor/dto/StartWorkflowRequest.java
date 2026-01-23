package io.temporal.conductor.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import java.util.Map;

/**
 * Request object for starting a new workflow execution.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StartWorkflowRequest {
    private String name;
    private Integer version;
    private String correlationId;
    private Map<String, Object> input;
    private Map<String, String> taskToDomain;
    private WorkflowDef workflowDef;
    private String externalInputPayloadStoragePath;
    private Integer priority;
    private String createdBy;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    public Map<String, Object> getInput() {
        return input;
    }

    public void setInput(Map<String, Object> input) {
        this.input = input;
    }

    public Map<String, String> getTaskToDomain() {
        return taskToDomain;
    }

    public void setTaskToDomain(Map<String, String> taskToDomain) {
        this.taskToDomain = taskToDomain;
    }

    public WorkflowDef getWorkflowDef() {
        return workflowDef;
    }

    public void setWorkflowDef(WorkflowDef workflowDef) {
        this.workflowDef = workflowDef;
    }

    public String getExternalInputPayloadStoragePath() {
        return externalInputPayloadStoragePath;
    }

    public void setExternalInputPayloadStoragePath(String path) {
        this.externalInputPayloadStoragePath = path;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }
}
