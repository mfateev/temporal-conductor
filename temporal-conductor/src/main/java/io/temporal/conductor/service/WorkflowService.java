package io.temporal.conductor.service;

import io.temporal.conductor.dto.SearchResult;
import io.temporal.conductor.dto.StartWorkflowRequest;
import io.temporal.conductor.dto.Workflow;
import io.temporal.conductor.dto.WorkflowStatus;
import io.temporal.conductor.dto.WorkflowSummary;
import java.util.List;

/**
 * Service interface for workflow operations.
 */
public interface WorkflowService {

    /**
     * Start a new workflow execution.
     *
     * @param request the workflow start request
     * @return the workflow ID
     */
    String startWorkflow(StartWorkflowRequest request);

    /**
     * Get a workflow by ID.
     *
     * @param workflowId the workflow ID
     * @param includeTasks whether to include tasks in the response
     * @return the workflow
     */
    Workflow getWorkflow(String workflowId, boolean includeTasks);

    /**
     * Get the status of a workflow.
     *
     * @param workflowId the workflow ID
     * @return the workflow status
     */
    WorkflowStatus getWorkflowStatus(String workflowId);

    /**
     * Terminate a running workflow.
     *
     * @param workflowId the workflow ID
     * @param reason the termination reason
     */
    void terminateWorkflow(String workflowId, String reason);

    /**
     * Pause a running workflow.
     *
     * @param workflowId the workflow ID
     */
    void pauseWorkflow(String workflowId);

    /**
     * Resume a paused workflow.
     *
     * @param workflowId the workflow ID
     */
    void resumeWorkflow(String workflowId);

    /**
     * Restart a workflow from the beginning.
     *
     * @param workflowId the workflow ID
     * @param useLatestDefinitions whether to use the latest workflow definitions
     * @return the new workflow ID
     */
    String restartWorkflow(String workflowId, boolean useLatestDefinitions);

    /**
     * Retry a failed workflow from the failed task.
     *
     * @param workflowId the workflow ID
     * @param resumeSubworkflowTasks whether to resume subworkflow tasks
     */
    void retryWorkflow(String workflowId, boolean resumeSubworkflowTasks);

    /**
     * Get running workflows by name.
     *
     * @param name the workflow name
     * @param version the workflow version (optional)
     * @param startTime the start time filter (optional)
     * @param endTime the end time filter (optional)
     * @return list of workflow IDs
     */
    List<String> getRunningWorkflows(String name, Integer version, Long startTime, Long endTime);

    /**
     * Search for workflows.
     *
     * @param start the start index
     * @param size the page size
     * @param sort the sort field
     * @param freeText free text search
     * @param query the search query
     * @return search results with workflow summaries
     */
    SearchResult<WorkflowSummary> searchWorkflows(
            int start, int size, String sort, String freeText, String query);

    /**
     * Search for workflows (v2 with full workflow objects).
     *
     * @param start the start index
     * @param size the page size
     * @param sort the sort field
     * @param freeText free text search
     * @param query the search query
     * @return search results with full workflow objects
     */
    SearchResult<Workflow> searchWorkflowsV2(
            int start, int size, String sort, String freeText, String query);
}
