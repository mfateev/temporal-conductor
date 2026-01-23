package io.temporal.conductor.dto;

import com.netflix.conductor.common.metadata.tasks.TaskResult;
import java.util.Map;

/**
 * Request DTO for updating task status.
 * This wraps the Conductor TaskResult for proper JSON deserialization.
 */
public class UpdateTaskRequest {

    private String taskId;
    private String workflowInstanceId;
    private String status;
    private Map<String, Object> outputData;
    private String reasonForIncompletion;
    private long callbackAfterSeconds;
    private String workerId;

    public UpdateTaskRequest() {
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getWorkflowInstanceId() {
        return workflowInstanceId;
    }

    public void setWorkflowInstanceId(String workflowInstanceId) {
        this.workflowInstanceId = workflowInstanceId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Map<String, Object> getOutputData() {
        return outputData;
    }

    public void setOutputData(Map<String, Object> outputData) {
        this.outputData = outputData;
    }

    public String getReasonForIncompletion() {
        return reasonForIncompletion;
    }

    public void setReasonForIncompletion(String reasonForIncompletion) {
        this.reasonForIncompletion = reasonForIncompletion;
    }

    public long getCallbackAfterSeconds() {
        return callbackAfterSeconds;
    }

    public void setCallbackAfterSeconds(long callbackAfterSeconds) {
        this.callbackAfterSeconds = callbackAfterSeconds;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    /**
     * Converts this request to a Conductor TaskResult.
     */
    public TaskResult toTaskResult() {
        TaskResult result = new TaskResult();
        result.setTaskId(taskId);
        result.setWorkflowInstanceId(workflowInstanceId);
        result.setOutputData(outputData);
        result.setReasonForIncompletion(reasonForIncompletion);
        result.setCallbackAfterSeconds(callbackAfterSeconds);
        result.setWorkerId(workerId);

        if (status != null) {
            try {
                result.setStatus(TaskResult.Status.valueOf(status));
            } catch (IllegalArgumentException e) {
                result.setStatus(TaskResult.Status.IN_PROGRESS);
            }
        }

        return result;
    }
}
