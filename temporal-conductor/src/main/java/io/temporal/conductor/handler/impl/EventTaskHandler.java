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
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import io.temporal.conductor.handler.ExecutionMode;
import io.temporal.conductor.handler.TaskExecutionContext;
import io.temporal.conductor.handler.TaskTypeHandler;
import io.temporal.conductor.workflow.model.TaskExecutionResult;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for EVENT tasks that publish events to a queue.
 *
 * <p>EVENT tasks publish an event message to a configured sink (queue).
 * The actual publishing is done via a Temporal activity.
 *
 * <p>Configuration:
 * <ul>
 *   <li>sink - Queue destination (required), format: "prefix:queueName"</li>
 *   <li>Additional input fields become part of the event payload</li>
 * </ul>
 *
 * <p>Output:
 * <ul>
 *   <li>All input fields</li>
 *   <li>workflowInstanceId, workflowType, workflowVersion, correlationId</li>
 *   <li>event_produced - The queue name where the event was published</li>
 * </ul>
 */
public class EventTaskHandler implements TaskTypeHandler {

    private static final Logger logger = LoggerFactory.getLogger(EventTaskHandler.class);

    /** Activity type name for event publishing. */
    private static final String EVENT_PUBLISH_ACTIVITY = "event-publish";

    @Override
    public String getTaskType() {
        return TaskType.EVENT.name();
    }

    @Override
    public ExecutionMode getExecutionMode(TaskModel task) {
        return ExecutionMode.ACTIVITY;
    }

    @Override
    public void execute(TaskModel task, WorkflowModel workflow, TaskExecutionContext context) {
        String taskRefName = task.getReferenceTaskName();
        String taskId = task.getTaskId();

        if (task.getStatus() == TaskModel.Status.SCHEDULED) {
            task.setStatus(TaskModel.Status.IN_PROGRESS);
            task.setStartTime(context.currentTimeMillis());
        }

        if (task.getStatus().isTerminal()) {
            return;
        }

        // Check if activity already pending
        if (context.isActivityPending(taskId)) {
            return;
        }

        // Prepare event payload (reusing Conductor's Event task logic)
        Map<String, Object> payload = new HashMap<>(
                task.getInputData() != null ? task.getInputData() : Collections.emptyMap());
        payload.put("workflowInstanceId", workflow.getWorkflowId());
        payload.put("workflowType", workflow.getWorkflowName());
        payload.put("workflowVersion", workflow.getWorkflowVersion());
        payload.put("correlationId", workflow.getCorrelationId());

        // Compute queue name from sink parameter (reusing Conductor's logic)
        String sinkValue = (String) task.getInputData().get("sink");
        if (sinkValue == null || sinkValue.isEmpty()) {
            failTask(task, "EVENT task requires 'sink' parameter", context);
            return;
        }

        String queueName = computeEventQueueName(sinkValue, workflow);

        // Set output data (includes payload and queue name)
        task.addOutput(payload);
        task.addOutput("event_produced", queueName);

        // Serialize payload to JSON for the activity
        String payloadJson;
        try {
            payloadJson = context.getObjectMapper().writeValueAsString(task.getOutputData());
        } catch (JsonProcessingException e) {
            failTask(task, "Failed to serialize event payload: " + e.getMessage(), context);
            return;
        }

        // Build activity input
        Map<String, Object> activityInput = new HashMap<>();
        activityInput.put("queueName", queueName);
        activityInput.put("taskId", taskId);
        activityInput.put("payloadJson", payloadJson);

        // Call activity to publish event
        context.executeActivityAsync(
                EVENT_PUBLISH_ACTIVITY,
                taskId,
                taskRefName,
                activityInput,
                (result, failure) -> handleEventPublishCompletion(task, queueName, result, failure, context));
    }

    /**
     * Handle event publish activity completion.
     */
    private void handleEventPublishCompletion(
            TaskModel task,
            String queueName,
            TaskExecutionResult result,
            Throwable failure,
            TaskExecutionContext context) {
        if (failure != null) {
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion("Failed to publish event: " + failure.getMessage());
            logger.error("EVENT task {} failed to publish to queue '{}': {}",
                    task.getReferenceTaskName(), queueName, failure.getMessage());
        } else {
            task.setStatus(TaskModel.Status.COMPLETED);
            logger.info("EVENT task {} published to queue '{}'", task.getReferenceTaskName(), queueName);
        }
        task.setEndTime(context.currentTimeMillis());
        context.markStateUpdated();
    }

    /**
     * Compute the event queue name from the sink value.
     *
     * <p>The sink format is "prefix:queueName" where prefix determines
     * the queue implementation (e.g., "conductor", "sqs", "kafka").
     *
     * @param sinkValue the sink configuration value
     * @param workflow the workflow model
     * @return the computed queue name
     */
    private String computeEventQueueName(String sinkValue, WorkflowModel workflow) {
        // Format: "prefix:queueName" or just "queueName"
        // Conductor uses the full sink value as the queue name
        // The EventQueueProvider parses the prefix to determine the queue type
        if (sinkValue.contains(":")) {
            return sinkValue;
        }
        // Default to conductor: prefix if not specified
        return "conductor:" + sinkValue;
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
