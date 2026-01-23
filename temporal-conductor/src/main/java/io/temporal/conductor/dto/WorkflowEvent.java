package io.temporal.conductor.dto;

import java.util.Map;

/**
 * Event DTO representing workflow and task lifecycle events.
 */
public class WorkflowEvent {

    /**
     * Event types for workflow and task lifecycle events.
     */
    public enum EventType {
        STARTED,
        PAUSED,
        RESUMED,
        COMPLETED,
        FAILED,
        TERMINATED,
        TASK_STARTED,
        TASK_COMPLETED,
        TASK_FAILED
    }

    private String eventId;
    private String workflowId;
    private EventType eventType;
    private long timestamp;
    private Map<String, Object> payload;
    private String taskId;
    private String taskRefName;

    public WorkflowEvent() {
    }

    private WorkflowEvent(Builder builder) {
        this.eventId = builder.eventId;
        this.workflowId = builder.workflowId;
        this.eventType = builder.eventType;
        this.timestamp = builder.timestamp;
        this.payload = builder.payload;
        this.taskId = builder.taskId;
        this.taskRefName = builder.taskRefName;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public void setPayload(Map<String, Object> payload) {
        this.payload = payload;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskRefName() {
        return taskRefName;
    }

    public void setTaskRefName(String taskRefName) {
        this.taskRefName = taskRefName;
    }

    /**
     * Builder for WorkflowEvent.
     */
    public static class Builder {
        private String eventId;
        private String workflowId;
        private EventType eventType;
        private long timestamp;
        private Map<String, Object> payload;
        private String taskId;
        private String taskRefName;

        public Builder eventId(String eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder workflowId(String workflowId) {
            this.workflowId = workflowId;
            return this;
        }

        public Builder eventType(EventType eventType) {
            this.eventType = eventType;
            return this;
        }

        public Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder payload(Map<String, Object> payload) {
            this.payload = payload;
            return this;
        }

        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder taskRefName(String taskRefName) {
            this.taskRefName = taskRefName;
            return this;
        }

        public WorkflowEvent build() {
            return new WorkflowEvent(this);
        }
    }
}
