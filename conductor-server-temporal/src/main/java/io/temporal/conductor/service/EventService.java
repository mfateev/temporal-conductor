package io.temporal.conductor.service;

import io.temporal.conductor.dto.WorkflowEvent;
import java.util.List;

/**
 * Service interface for event operations.
 */
public interface EventService {

    /**
     * Get workflow lifecycle events.
     *
     * @param workflowId the workflow ID
     * @param start the start index for pagination
     * @param size the page size
     * @return list of workflow events
     */
    List<WorkflowEvent> getWorkflowEvents(String workflowId, int start, int size);

    /**
     * Get task events.
     *
     * @param taskId the task ID
     * @return list of task events
     */
    List<WorkflowEvent> getTaskEvents(String taskId);

    /**
     * Record a new event (for internal use).
     *
     * @param event the event to record
     */
    void recordEvent(WorkflowEvent event);
}
