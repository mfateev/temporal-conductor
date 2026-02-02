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

package io.temporal.conductor.handler.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import io.temporal.conductor.handler.ExecutionMode;
import io.temporal.conductor.handler.TaskExecutionContext;
import io.temporal.conductor.handler.TaskTypeHandler;
import io.temporal.conductor.workflow.model.ConductorWorkflowInput;
import io.temporal.workflow.ChildWorkflowOptions;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for SUB_WORKFLOW tasks that start child workflows and wait for completion.
 *
 * <p>SUB_WORKFLOW tasks execute a nested workflow definition. The parent workflow
 * waits for the child to complete before continuing. The child's output becomes
 * the task's output.
 *
 * <p>Configuration options:
 * <ul>
 *   <li>subWorkflowName + subWorkflowVersion - Reference to registered workflow</li>
 *   <li>subWorkflowDefinition - Inline workflow definition</li>
 *   <li>workflowInput - Input data for the child workflow</li>
 * </ul>
 */
public class SubWorkflowTaskHandler implements TaskTypeHandler {

    private static final Logger logger = LoggerFactory.getLogger(SubWorkflowTaskHandler.class);

    @Override
    public String getTaskType() {
        return TaskType.SUB_WORKFLOW.name();
    }

    @Override
    public ExecutionMode getExecutionMode(TaskModel task) {
        return ExecutionMode.CHILD_WORKFLOW;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(TaskModel task, WorkflowModel workflow, TaskExecutionContext context) {
        String taskRefName = task.getReferenceTaskName();
        String taskId = task.getTaskId();

        if (task.getStatus() == TaskModel.Status.SCHEDULED) {
            task.setStatus(TaskModel.Status.IN_PROGRESS);
            task.setStartTime(context.currentTimeMillis());
        }

        // If child workflow already started, nothing more to do - callback will complete it
        if (context.isChildWorkflowPending(taskId)) {
            return;
        }

        Map<String, Object> inputData = task.getInputData();
        if (inputData == null) {
            failTask(task, "SUB_WORKFLOW task has no input data", context);
            return;
        }

        // Extract sub-workflow parameters from task input
        String subWorkflowName = (String) inputData.get("subWorkflowName");
        Integer subWorkflowVersion = inputData.get("subWorkflowVersion") != null
                ? ((Number) inputData.get("subWorkflowVersion")).intValue() : null;
        Object subWorkflowDefinitionObj = inputData.get("subWorkflowDefinition");
        @SuppressWarnings("unchecked")
        Map<String, Object> subWorkflowInput = (Map<String, Object>) inputData.get("workflowInput");

        if (subWorkflowInput == null) {
            subWorkflowInput = Collections.emptyMap();
        }

        // Resolve the sub-workflow definition
        WorkflowDef subWorkflowDef = resolveWorkflowDefinition(
                task, subWorkflowDefinitionObj, subWorkflowName, subWorkflowVersion, context);
        if (subWorkflowDef == null) {
            return; // Task already failed in resolveWorkflowDefinition
        }

        // Build child workflow input
        String subWorkflowDefJson;
        try {
            subWorkflowDefJson = context.getObjectMapper().writeValueAsString(subWorkflowDef);
        } catch (JsonProcessingException e) {
            failTask(task, "Failed to serialize sub-workflow definition: " + e.getMessage(), context);
            return;
        }

        ConductorWorkflowInput workflowInput = context.getWorkflowInput();
        ConductorWorkflowInput childInput = ConductorWorkflowInput.builder()
                .workflowDefJson(subWorkflowDefJson)
                .workflowInput(subWorkflowInput)
                .taskDefsJson(workflowInput.getTaskDefsJson())
                .workflowDefsJson(workflowInput.getWorkflowDefsJson())
                .correlationId(workflowInput.getCorrelationId())
                .build();

        // Generate child workflow ID: parentWorkflowId-taskRefName
        String childWorkflowId = context.getWorkflowId() + "-" + taskRefName;

        // Create child workflow options
        ChildWorkflowOptions options = ChildWorkflowOptions.newBuilder()
                .setWorkflowId(childWorkflowId)
                .setTaskQueue(context.getTaskQueue())
                .build();

        // Start child workflow asynchronously
        String childWorkflowType = subWorkflowDef.getName();
        context.startChildWorkflow(childWorkflowId, childWorkflowType, childInput, options, taskId,
                (result, failure) -> handleChildWorkflowCompletion(task, result, failure, context));

        logger.info("Started child workflow {} for SUB_WORKFLOW task {}", childWorkflowId, taskRefName);
    }

    /**
     * Handle child workflow completion.
     */
    private void handleChildWorkflowCompletion(
            TaskModel task,
            Map<String, Object> result,
            Throwable failure,
            TaskExecutionContext context) {
        if (failure != null) {
            logger.error("Child workflow failed for SUB_WORKFLOW task {}: {}",
                    task.getReferenceTaskName(), failure.getMessage());
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion("Child workflow failed: " + failure.getMessage());
        } else {
            task.setOutputData(result);
            task.setStatus(TaskModel.Status.COMPLETED);
            logger.info("Child workflow completed for SUB_WORKFLOW task {}",
                    task.getReferenceTaskName());
        }
        task.setEndTime(context.currentTimeMillis());
        context.markStateUpdated();
    }

    /**
     * Resolve the workflow definition from inline definition or metadata.
     */
    @SuppressWarnings("unchecked")
    private WorkflowDef resolveWorkflowDefinition(
            TaskModel task,
            Object subWorkflowDefinitionObj,
            String subWorkflowName,
            Integer subWorkflowVersion,
            TaskExecutionContext context) {

        if (subWorkflowDefinitionObj != null) {
            // Inline workflow definition provided - can be WorkflowDef or Map
            if (subWorkflowDefinitionObj instanceof WorkflowDef) {
                return (WorkflowDef) subWorkflowDefinitionObj;
            } else if (subWorkflowDefinitionObj instanceof Map) {
                try {
                    String defJson = context.getObjectMapper().writeValueAsString(subWorkflowDefinitionObj);
                    return context.getObjectMapper().readValue(defJson, WorkflowDef.class);
                } catch (JsonProcessingException e) {
                    failTask(task, "Failed to parse inline subWorkflowDefinition: " + e.getMessage(), context);
                    return null;
                }
            } else {
                failTask(task, "Invalid subWorkflowDefinition type: " + subWorkflowDefinitionObj.getClass().getName(), context);
                return null;
            }
        } else if (subWorkflowName != null) {
            // Look up workflow definition from metadata
            Optional<WorkflowDef> defOpt = context.getWorkflowDef(subWorkflowName, subWorkflowVersion);

            if (defOpt.isEmpty()) {
                failTask(task, "Sub-workflow definition not found: " + subWorkflowName
                        + (subWorkflowVersion != null ? " version " + subWorkflowVersion : ""), context);
                return null;
            }
            return defOpt.get();
        } else {
            failTask(task, "SUB_WORKFLOW task requires either subWorkflowName or subWorkflowDefinition", context);
            return null;
        }
    }

    /**
     * Mark a task as failed with the given reason.
     */
    private void failTask(TaskModel task, String reason, TaskExecutionContext context) {
        task.setStatus(TaskModel.Status.FAILED);
        task.setReasonForIncompletion(reason);
        task.setEndTime(context.currentTimeMillis());
    }
}
