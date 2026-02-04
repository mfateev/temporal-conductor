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

package io.temporal.conductor.activity;

import com.netflix.conductor.common.metadata.tasks.TaskResult;
import com.netflix.conductor.common.metadata.workflow.RerunWorkflowRequest;
import com.netflix.conductor.common.metadata.workflow.SkipTaskRequest;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.StartWorkflowInput;
import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import io.temporal.conductor.workflow.model.TaskExecutionResult;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of ExtensionTaskExecutionActivity that executes Conductor extension tasks.
 *
 * <p>This activity discovers and executes WorkflowSystemTask implementations that are
 * registered via Spring. Extension tasks like KAFKA_PUBLISH or JSON_JQ_TRANSFORM are
 * automatically discovered when their dependencies are added to the classpath.
 *
 * <p>The execution follows Conductor's standard task execution pattern:
 * <ol>
 *   <li>Create a minimal WorkflowModel and TaskModel from input</li>
 *   <li>Call the extension task's start() and execute() methods</li>
 *   <li>Extract the output data and status from the TaskModel</li>
 * </ol>
 *
 * <p>Note: This class is not annotated with @Component. It is created as a bean by
 * {@link io.temporal.conductor.config.ExtensionTaskDiscoveryConfig} with the discovered
 * extension tasks injected.
 */
public class ExtensionTaskExecutionActivityImpl implements ExtensionTaskExecutionActivity {

    private static final Logger logger = LoggerFactory.getLogger(ExtensionTaskExecutionActivityImpl.class);

    private final Map<String, WorkflowSystemTask> extensionTasks;

    /**
     * Create an ExtensionTaskExecutionActivityImpl with discovered extension tasks.
     *
     * @param extensionTasks map of task type to WorkflowSystemTask implementation
     */
    public ExtensionTaskExecutionActivityImpl(Map<String, WorkflowSystemTask> extensionTasks) {
        this.extensionTasks = extensionTasks != null ? extensionTasks : new HashMap<>();
        logger.info("Initialized ExtensionTaskExecutionActivity with {} extension tasks: {}",
                this.extensionTasks.size(), this.extensionTasks.keySet());
    }

    /**
     * Create an ExtensionTaskExecutionActivityImpl with no extension tasks.
     * Extension tasks can be registered later.
     */
    public ExtensionTaskExecutionActivityImpl() {
        this.extensionTasks = new HashMap<>();
        logger.info("Initialized ExtensionTaskExecutionActivity with no extension tasks");
    }

    @Override
    public TaskExecutionResult execute(String taskType, String taskRefName, Map<String, Object> inputData) {
        logger.info("Executing extension task: type={}, refName={}", taskType, taskRefName);

        WorkflowSystemTask extensionTask = extensionTasks.get(taskType);
        if (extensionTask == null) {
            logger.error("No extension task registered for type: {}", taskType);
            return new TaskExecutionResult(
                    Collections.emptyMap(),
                    "FAILED",
                    "No extension task registered for type: " + taskType);
        }

        try {
            // Build a minimal TaskModel
            TaskModel taskModel = new TaskModel();
            taskModel.setTaskId("ext-" + taskRefName);
            taskModel.setReferenceTaskName(taskRefName);
            taskModel.setTaskType(taskType);
            taskModel.setInputData(inputData != null ? inputData : new HashMap<>());
            taskModel.setStatus(TaskModel.Status.SCHEDULED);
            taskModel.setStartTime(System.currentTimeMillis());

            // Build a minimal WorkflowModel (some tasks may need workflow context)
            WorkflowModel workflowModel = new WorkflowModel();
            workflowModel.setWorkflowId("ext-workflow");
            workflowModel.setStatus(WorkflowModel.Status.RUNNING);

            // Execute the extension task using a no-op executor
            // (extension tasks typically don't need to schedule additional tasks)
            NoOpWorkflowExecutor noOpExecutor = new NoOpWorkflowExecutor();

            // Call start() first
            extensionTask.start(workflowModel, taskModel, noOpExecutor);

            // If still not terminal, call execute()
            if (!taskModel.getStatus().isTerminal()) {
                extensionTask.execute(workflowModel, taskModel, noOpExecutor);
            }

            // Extract result
            taskModel.setEndTime(System.currentTimeMillis());

            if (taskModel.getStatus() == TaskModel.Status.COMPLETED) {
                logger.info("Extension task {} completed successfully", taskRefName);
                return new TaskExecutionResult(
                        taskModel.getOutputData() != null ? taskModel.getOutputData() : new HashMap<>(),
                        "COMPLETED",
                        null);
            } else {
                String reason = taskModel.getReasonForIncompletion();
                logger.warn("Extension task {} failed: {}", taskRefName, reason);
                return new TaskExecutionResult(
                        Collections.emptyMap(),
                        "FAILED",
                        reason != null ? reason : "Task did not complete");
            }

        } catch (Exception e) {
            logger.error("Extension task {} threw exception", taskRefName, e);
            return new TaskExecutionResult(
                    Collections.emptyMap(),
                    "FAILED",
                    "Exception: " + e.getMessage());
        }
    }

    /**
     * Register an extension task for a specific task type.
     *
     * @param taskType the task type name
     * @param task the WorkflowSystemTask implementation
     */
    public void registerExtensionTask(String taskType, WorkflowSystemTask task) {
        extensionTasks.put(taskType, task);
        logger.info("Registered extension task for type: {}", taskType);
    }

    /**
     * Get the set of registered extension task types.
     *
     * @return set of registered task type names
     */
    public java.util.Set<String> getRegisteredTaskTypes() {
        return java.util.Collections.unmodifiableSet(extensionTasks.keySet());
    }

    /**
     * No-op WorkflowExecutor implementation for extension task execution.
     * Extension tasks typically don't need to schedule additional tasks or modify workflow state.
     */
    private static class NoOpWorkflowExecutor implements WorkflowExecutor {

        @Override
        public void scheduleNextIteration(TaskModel loopTask, WorkflowModel workflow) {
            // No-op
        }

        @Override
        public void resetCallbacksForWorkflow(String workflowId) {
            // No-op
        }

        @Override
        public String rerun(RerunWorkflowRequest request) {
            throw new UnsupportedOperationException("Not supported in extension task context");
        }

        @Override
        public void restart(String workflowId, boolean useLatestDefinitions) {
            throw new UnsupportedOperationException("Not supported in extension task context");
        }

        @Override
        public void retry(String workflowId, boolean resumeSubworkflowTasks) {
            throw new UnsupportedOperationException("Not supported in extension task context");
        }

        @Override
        public TaskModel updateTask(TaskResult taskResult) {
            throw new UnsupportedOperationException("Not supported in extension task context");
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
            throw new UnsupportedOperationException("Not supported in extension task context");
        }

        @Override
        public WorkflowModel decideWithLock(WorkflowModel workflow) {
            return null;
        }

        @Override
        public void terminateWorkflow(String workflowId, String reason) {
            // No-op
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
            // No-op
        }

        @Override
        public void resumeWorkflow(String workflowId) {
            // No-op
        }

        @Override
        public void skipTaskFromWorkflow(
                String workflowId, String taskReferenceName, SkipTaskRequest skipTaskRequest) {
            throw new UnsupportedOperationException("Not supported in extension task context");
        }

        @Override
        public WorkflowModel getWorkflow(String workflowId, boolean includeTasks) {
            return null;
        }

        @Override
        public String startWorkflow(StartWorkflowInput input) {
            throw new UnsupportedOperationException("Not supported in extension task context");
        }
    }
}
