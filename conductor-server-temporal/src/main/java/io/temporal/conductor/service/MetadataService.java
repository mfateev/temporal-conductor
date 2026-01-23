package io.temporal.conductor.service;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import java.util.List;

/**
 * Service interface for metadata operations (workflow and task definitions).
 */
public interface MetadataService {

    // Workflow definitions

    /**
     * Get all workflow definitions.
     *
     * @return list of all workflow definitions
     */
    List<WorkflowDef> getAllWorkflowDefs();

    /**
     * Get a specific version of a workflow definition.
     *
     * @param name the workflow name
     * @param version the workflow version
     * @return the workflow definition
     */
    WorkflowDef getWorkflowDef(String name, int version);

    /**
     * Get the latest version of a workflow definition.
     *
     * @param name the workflow name
     * @return the workflow definition
     */
    WorkflowDef getLatestWorkflowDef(String name);

    /**
     * Register a new workflow definition.
     *
     * @param workflowDef the workflow definition
     */
    void registerWorkflowDef(WorkflowDef workflowDef);

    /**
     * Update existing workflow definitions.
     *
     * @param workflowDefs the workflow definitions to update
     */
    void updateWorkflowDefs(List<WorkflowDef> workflowDefs);

    /**
     * Delete a workflow definition.
     *
     * @param name the workflow name
     * @param version the workflow version
     */
    void deleteWorkflowDef(String name, int version);

    // Task definitions

    /**
     * Get all task definitions.
     *
     * @return list of all task definitions
     */
    List<TaskDef> getAllTaskDefs();

    /**
     * Get a task definition by type.
     *
     * @param taskType the task type
     * @return the task definition
     */
    TaskDef getTaskDef(String taskType);

    /**
     * Register new task definitions.
     *
     * @param taskDefs the task definitions to register
     */
    void registerTaskDefs(List<TaskDef> taskDefs);

    /**
     * Delete a task definition.
     *
     * @param taskType the task type
     */
    void deleteTaskDef(String taskType);
}
