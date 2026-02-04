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
import io.temporal.api.enums.v1.ParentClosePolicy;
import io.temporal.conductor.handler.ExecutionMode;
import io.temporal.conductor.handler.TaskExecutionContext;
import io.temporal.conductor.handler.TaskTypeHandler;
import io.temporal.conductor.workflow.model.ConductorWorkflowInput;
import io.temporal.workflow.ChildWorkflowOptions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for START_WORKFLOW tasks (fire-and-forget child workflow).
 *
 * <p>Unlike SUB_WORKFLOW, this task completes immediately after starting
 * the child workflow. The child workflow continues independently even if
 * the parent completes or fails.
 *
 * <p>Configuration (nested in "startWorkflow" object):
 * <ul>
 *   <li>name - Workflow name (required)</li>
 *   <li>version - Workflow version (optional)</li>
 *   <li>input - Input data for the child workflow</li>
 *   <li>correlationId - Correlation ID for tracing</li>
 * </ul>
 */
public class StartWorkflowTaskHandler implements TaskTypeHandler {

    private static final Logger logger = LoggerFactory.getLogger(StartWorkflowTaskHandler.class);

    @Override
    public String getTaskType() {
        return TaskType.START_WORKFLOW.name();
    }

    @Override
    public ExecutionMode getExecutionMode(TaskModel task) {
        return ExecutionMode.FIRE_AND_FORGET_CHILD;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void execute(TaskModel task, WorkflowModel workflow, TaskExecutionContext context) {
        String taskRefName = task.getReferenceTaskName();

        if (task.getStatus() == TaskModel.Status.SCHEDULED) {
            task.setStatus(TaskModel.Status.IN_PROGRESS);
            task.setStartTime(context.currentTimeMillis());
        }

        // START_WORKFLOW completes immediately - no tracking of child workflow
        if (task.getStatus().isTerminal()) {
            return;
        }

        Map<String, Object> inputData = task.getInputData();
        if (inputData == null) {
            failTask(task, "START_WORKFLOW task has no input data", context);
            return;
        }

        // Extract start workflow parameters from task input
        // START_WORKFLOW uses "startWorkflow" nested object for configuration
        @SuppressWarnings("unchecked")
        Map<String, Object> startWorkflowConfig = (Map<String, Object>) inputData.get("startWorkflow");
        if (startWorkflowConfig == null) {
            // Fall back to direct parameters (alternative input format)
            startWorkflowConfig = inputData;
        }

        String workflowName = (String) startWorkflowConfig.get("name");
        Integer workflowVersion = startWorkflowConfig.get("version") != null
                ? ((Number) startWorkflowConfig.get("version")).intValue() : null;
        @SuppressWarnings("unchecked")
        Map<String, Object> childWorkflowInput = (Map<String, Object>) startWorkflowConfig.get("input");
        String correlationId = (String) startWorkflowConfig.get("correlationId");

        if (workflowName == null) {
            failTask(task, "START_WORKFLOW task requires 'name' parameter", context);
            return;
        }

        if (childWorkflowInput == null) {
            childWorkflowInput = Collections.emptyMap();
        }

        // Look up workflow definition from metadata
        Optional<WorkflowDef> defOpt = context.getWorkflowDef(workflowName, workflowVersion);

        if (defOpt.isEmpty()) {
            failTask(task, "Workflow definition not found: " + workflowName
                    + (workflowVersion != null ? " version " + workflowVersion : ""), context);
            return;
        }

        WorkflowDef childWorkflowDef = defOpt.get();

        // Build child workflow input
        String childWorkflowDefJson;
        try {
            childWorkflowDefJson = context.getObjectMapper().writeValueAsString(childWorkflowDef);
        } catch (JsonProcessingException e) {
            failTask(task, "Failed to serialize workflow definition: " + e.getMessage(), context);
            return;
        }

        ConductorWorkflowInput workflowInput = context.getWorkflowInput();
        ConductorWorkflowInput childInput = ConductorWorkflowInput.builder()
                .workflowDefJson(childWorkflowDefJson)
                .workflowInput(childWorkflowInput)
                .taskDefsJson(workflowInput.getTaskDefsJson())
                .workflowDefsJson(workflowInput.getWorkflowDefsJson())
                .correlationId(correlationId != null ? correlationId : workflowInput.getCorrelationId())
                .build();

        // Generate child workflow ID: parentWorkflowId-taskRefName
        String childWorkflowId = context.getWorkflowId() + "-" + taskRefName;

        // Create child workflow options with ABANDON policy - fire-and-forget
        // The child workflow will continue running even if parent completes or fails
        ChildWorkflowOptions options = ChildWorkflowOptions.newBuilder()
                .setWorkflowId(childWorkflowId)
                .setTaskQueue(context.getTaskQueue())
                .setParentClosePolicy(ParentClosePolicy.PARENT_CLOSE_POLICY_ABANDON)
                .build();

        // Start child workflow - fire and forget
        String childWorkflowType = childWorkflowDef.getName();
        context.startFireAndForgetChildWorkflow(childWorkflowId, childWorkflowType, childInput, options);

        logger.info("Started fire-and-forget child workflow {} for START_WORKFLOW task {}",
                childWorkflowId, taskRefName);

        // Complete the task immediately with the child workflow ID as output
        Map<String, Object> outputData = new HashMap<>();
        outputData.put("workflowId", childWorkflowId);
        outputData.put("workflowType", childWorkflowType);
        task.setOutputData(outputData);
        task.setStatus(TaskModel.Status.COMPLETED);
        task.setEndTime(context.currentTimeMillis());
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
