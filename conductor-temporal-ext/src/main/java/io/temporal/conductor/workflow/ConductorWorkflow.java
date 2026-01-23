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

package io.temporal.conductor.workflow;

import io.temporal.conductor.workflow.model.ConductorWorkflowInput;
import io.temporal.conductor.workflow.model.ConductorWorkflowOutput;
import io.temporal.conductor.workflow.model.TaskState;
import io.temporal.conductor.workflow.model.WorkflowState;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.util.List;
import java.util.Map;

/**
 * Temporal workflow interface for executing Conductor workflow definitions.
 *
 * <p>This workflow runs the Conductor DeciderService scheduling loop within
 * Temporal's durable execution framework.
 */
@WorkflowInterface
public interface ConductorWorkflow {

    /**
     * Execute a Conductor workflow definition.
     *
     * @param input Workflow definition, input parameters, and task definitions
     * @return Workflow output including status and task outputs
     */
    @WorkflowMethod
    ConductorWorkflowOutput execute(ConductorWorkflowInput input);

    // === Signal Methods ===

    /**
     * Signal to manually complete a task that's waiting for external input.
     *
     * @param taskRefName Reference name of the task to complete
     * @param output Output data to set on the task
     */
    @SignalMethod
    void completeTask(String taskRefName, Map<String, Object> output);

    /**
     * Signal to pause the workflow execution.
     */
    @SignalMethod
    void pause();

    /**
     * Signal to resume a paused workflow.
     */
    @SignalMethod
    void resume();

    /**
     * Signal to retry a failed task.
     *
     * @param taskRefName Reference name of the failed task to retry
     */
    @SignalMethod
    void retryFailedTask(String taskRefName);

    // === Query Methods ===

    /**
     * Query to get the full workflow state.
     *
     * @return The current workflow state including all task details
     */
    @QueryMethod
    WorkflowState getWorkflow();

    /**
     * Query to get all task states.
     *
     * @return List of all task states
     */
    @QueryMethod
    List<TaskState> getTasks();

    /**
     * Query to get workflow variables.
     *
     * @return Map of workflow variables
     */
    @QueryMethod
    Map<String, Object> getVariables();
}
