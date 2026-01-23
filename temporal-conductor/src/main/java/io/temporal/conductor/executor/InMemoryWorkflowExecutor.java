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

package io.temporal.conductor.executor;

import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.metadata.workflow.RerunWorkflowRequest;
import com.netflix.conductor.common.metadata.workflow.SkipTaskRequest;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.common.utils.TaskUtils;
import com.netflix.conductor.core.execution.DeciderService;
import com.netflix.conductor.core.execution.StartWorkflowInput;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory WorkflowExecutor for use inside Temporal workflows.
 *
 * <p>This implementation keeps all state in the WorkflowModel, which Temporal
 * makes durable through its replay mechanism. No external database persistence
 * is needed.
 *
 * <p>Key method:
 * <ul>
 *   <li>scheduleNextIteration(): Used by DoWhile to schedule the next loop iteration</li>
 * </ul>
 *
 * <p>Design principles:
 * <ul>
 *   <li>All state lives in WorkflowModel (Temporal makes this durable)</li>
 *   <li>Uses Workflow.currentTimeMillis() for deterministic timestamps</li>
 *   <li>Delegates task scheduling to DeciderService</li>
 *   <li>No external database or queue interactions</li>
 * </ul>
 */
public class InMemoryWorkflowExecutor implements WorkflowExecutor {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryWorkflowExecutor.class);

    private final DeciderService deciderService;

    /**
     * Creates a new InMemoryWorkflowExecutor.
     *
     * @param deciderService the DeciderService to use for task scheduling
     */
    public InMemoryWorkflowExecutor(DeciderService deciderService) {
        this.deciderService = deciderService;
    }

    /**
     * Schedule the next iteration of a DO_WHILE loop.
     *
     * <p>This is the key method needed by Conductor's DoWhile system task.
     * It schedules the first task in the loopOver list for the current iteration.
     * The DeciderService will handle scheduling the rest when this task completes.
     */
    @Override
    public void scheduleNextIteration(TaskModel loopTask, WorkflowModel workflow) {
        WorkflowTask workflowTask = loopTask.getWorkflowTask();
        if (workflowTask == null) {
            logger.warn("No workflow task defined for DO_WHILE task: {}",
                    loopTask.getReferenceTaskName());
            return;
        }

        List<WorkflowTask> loopOverTasks = workflowTask.getLoopOver();
        if (loopOverTasks == null || loopOverTasks.isEmpty()) {
            logger.warn("No loopOver tasks defined for DO_WHILE task: {}",
                    loopTask.getReferenceTaskName());
            return;
        }

        logger.debug("Scheduling next iteration {} for DO_WHILE task: {}",
                loopTask.getIteration(), loopTask.getReferenceTaskName());

        // Use DeciderService to create task models for the first loopOver task
        List<TaskModel> tasksToSchedule = deciderService.getTasksToBeScheduled(
                workflow,
                loopOverTasks.get(0),
                loopTask.getRetryCount(),
                null
        );

        // Append iteration number to task reference names and set iteration
        for (TaskModel task : tasksToSchedule) {
            task.setReferenceTaskName(TaskUtils.appendIteration(
                    task.getReferenceTaskName(),
                    loopTask.getIteration()
            ));
            task.setIteration(loopTask.getIteration());
            task.setScheduledTime(io.temporal.workflow.Workflow.currentTimeMillis());
            task.setStatus(TaskModel.Status.SCHEDULED);
        }

        // Add tasks to workflow model (Temporal makes this durable)
        workflow.getTasks().addAll(tasksToSchedule);

        logger.debug("Scheduled {} tasks for iteration {} of DO_WHILE task: {}",
                tasksToSchedule.size(), loopTask.getIteration(), loopTask.getReferenceTaskName());
    }

    // ===== Methods not needed for system tasks - no-op or throw =====

    @Override
    public void resetCallbacksForWorkflow(String workflowId) {
        // No-op: Temporal handles callbacks differently
    }

    @Override
    public String rerun(RerunWorkflowRequest request) {
        throw new UnsupportedOperationException("Use Temporal workflow retry instead");
    }

    @Override
    public void restart(String workflowId, boolean useLatestDefinitions) {
        throw new UnsupportedOperationException("Use Temporal workflow restart instead");
    }

    @Override
    public void retry(String workflowId, boolean resumeSubworkflowTasks) {
        throw new UnsupportedOperationException("Use Temporal workflow retry instead");
    }

    @Override
    public TaskModel updateTask(TaskResult taskResult) {
        throw new UnsupportedOperationException("Tasks are updated directly in workflow code");
    }

    @Override
    public TaskModel getTask(String taskId) {
        return null;
    }

    @Override
    public List<Workflow> getRunningWorkflows(String workflowName, int version) {
        return Collections.emptyList();
    }

    @Override
    public List<String> getWorkflows(String name, Integer version, Long startTime, Long endTime) {
        return Collections.emptyList();
    }

    @Override
    public List<String> getRunningWorkflowIds(String workflowName, int version) {
        return Collections.emptyList();
    }

    @Override
    public WorkflowModel decide(String workflowId) {
        throw new UnsupportedOperationException(
                "Use deciderService.decide() directly in workflow code");
    }

    @Override
    public WorkflowModel decideWithLock(WorkflowModel workflow) {
        return null;
    }

    @Override
    public void terminateWorkflow(String workflowId, String reason) {
        // Handled by workflow code setting status directly
    }

    @Override
    public WorkflowModel terminateWorkflow(
            WorkflowModel workflow, String reason, String failureWorkflow) {
        workflow.setStatus(WorkflowModel.Status.TERMINATED);
        workflow.setReasonForIncompletion(reason);
        return workflow;
    }

    @Override
    public void pauseWorkflow(String workflowId) {
        // Not supported in Temporal context
    }

    @Override
    public void resumeWorkflow(String workflowId) {
        // Not supported in Temporal context
    }

    @Override
    public void skipTaskFromWorkflow(
            String workflowId, String taskReferenceName, SkipTaskRequest skipTaskRequest) {
        throw new UnsupportedOperationException("Task skipping not supported in Temporal context");
    }

    @Override
    public WorkflowModel getWorkflow(String workflowId, boolean includeTasks) {
        return null;
    }

    @Override
    public String startWorkflow(StartWorkflowInput input) {
        throw new UnsupportedOperationException("Use Temporal child workflow instead");
    }
}
