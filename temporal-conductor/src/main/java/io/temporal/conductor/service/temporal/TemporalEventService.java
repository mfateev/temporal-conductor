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

package io.temporal.conductor.service.temporal;

import com.google.protobuf.Timestamp;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.History;
import io.temporal.api.history.v1.HistoryEvent;
import io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryRequest;
import io.temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryResponse;
import io.temporal.client.WorkflowClient;
import io.temporal.conductor.dto.WorkflowEvent;
import io.temporal.conductor.service.EventService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

/**
 * Temporal-backed implementation of EventService.
 *
 * <p>This service queries Temporal workflow history to extract
 * lifecycle events and maps them to Conductor event format.
 */
@Service
@Profile("temporal")
public class TemporalEventService implements EventService {

    private static final Logger logger = LoggerFactory.getLogger(TemporalEventService.class);

    private final WorkflowClient workflowClient;
    private final String namespace;

    /**
     * Creates a new TemporalEventService.
     *
     * @param workflowClient the Temporal workflow client
     * @param namespace the Temporal namespace
     */
    public TemporalEventService(
            WorkflowClient workflowClient,
            @Value("${temporal.namespace:conductor}") String namespace) {
        this.workflowClient = workflowClient;
        this.namespace = namespace;
    }

    @Override
    public List<WorkflowEvent> getWorkflowEvents(String workflowId, int start, int size) {
        logger.debug("Getting workflow events: workflowId={}, start={}, size={}",
                workflowId, start, size);

        try {
            GetWorkflowExecutionHistoryRequest request = GetWorkflowExecutionHistoryRequest
                    .newBuilder()
                    .setNamespace(namespace)
                    .setExecution(WorkflowExecution.newBuilder()
                            .setWorkflowId(workflowId)
                            .build())
                    .build();

            GetWorkflowExecutionHistoryResponse response = workflowClient.getWorkflowServiceStubs()
                    .blockingStub()
                    .getWorkflowExecutionHistory(request);

            History history = response.getHistory();
            List<WorkflowEvent> events = mapHistoryToEvents(workflowId, history);

            // Apply pagination
            if (start >= events.size()) {
                return Collections.emptyList();
            }
            int end = Math.min(start + size, events.size());
            return new ArrayList<>(events.subList(start, end));

        } catch (Exception e) {
            logger.warn("Failed to get workflow events for {}: {}", workflowId, e.getMessage());
            return Collections.emptyList();
        }
    }

    @Override
    public List<WorkflowEvent> getTaskEvents(String taskId) {
        logger.debug("Getting task events: taskId={}", taskId);

        // Extract workflow ID from task ID (assuming format: workflowId-task-N or workflowId-N)
        String workflowId = extractWorkflowId(taskId);
        if (workflowId == null) {
            logger.warn("Could not extract workflow ID from task ID: {}", taskId);
            return Collections.emptyList();
        }

        try {
            // Get all workflow events and filter by task ID
            List<WorkflowEvent> allEvents = getWorkflowEvents(workflowId, 0, Integer.MAX_VALUE);
            return allEvents.stream()
                    .filter(event -> taskId.equals(event.getTaskId()))
                    .toList();

        } catch (Exception e) {
            logger.warn("Failed to get task events for {}: {}", taskId, e.getMessage());
            return Collections.emptyList();
        }
    }

    @Override
    public void recordEvent(WorkflowEvent event) {
        // In Temporal, events are automatically recorded in workflow history
        // This method is a no-op for the Temporal implementation
        logger.debug("recordEvent called for Temporal service - no-op (events are in history)");
    }

    private List<WorkflowEvent> mapHistoryToEvents(String workflowId, History history) {
        List<WorkflowEvent> events = new ArrayList<>();

        for (HistoryEvent historyEvent : history.getEventsList()) {
            WorkflowEvent event = mapHistoryEvent(workflowId, historyEvent);
            if (event != null) {
                events.add(event);
            }
        }

        return events;
    }

    private WorkflowEvent mapHistoryEvent(String workflowId, HistoryEvent historyEvent) {
        EventType eventType = historyEvent.getEventType();
        long timestamp = timestampToMillis(historyEvent.getEventTime());

        return switch (eventType) {
            case EVENT_TYPE_WORKFLOW_EXECUTION_STARTED -> WorkflowEvent.builder()
                    .eventId(generateEventId(historyEvent))
                    .workflowId(workflowId)
                    .eventType(WorkflowEvent.EventType.STARTED)
                    .timestamp(timestamp)
                    .payload(Map.of(
                            "workflowType", historyEvent.getWorkflowExecutionStartedEventAttributes()
                                    .getWorkflowType().getName(),
                            "eventId", historyEvent.getEventId()))
                    .build();

            case EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED -> WorkflowEvent.builder()
                    .eventId(generateEventId(historyEvent))
                    .workflowId(workflowId)
                    .eventType(WorkflowEvent.EventType.COMPLETED)
                    .timestamp(timestamp)
                    .payload(Map.of("eventId", historyEvent.getEventId()))
                    .build();

            case EVENT_TYPE_WORKFLOW_EXECUTION_FAILED -> WorkflowEvent.builder()
                    .eventId(generateEventId(historyEvent))
                    .workflowId(workflowId)
                    .eventType(WorkflowEvent.EventType.FAILED)
                    .timestamp(timestamp)
                    .payload(Map.of(
                            "failure", historyEvent.getWorkflowExecutionFailedEventAttributes()
                                    .getFailure().getMessage(),
                            "eventId", historyEvent.getEventId()))
                    .build();

            case EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED -> WorkflowEvent.builder()
                    .eventId(generateEventId(historyEvent))
                    .workflowId(workflowId)
                    .eventType(WorkflowEvent.EventType.TERMINATED)
                    .timestamp(timestamp)
                    .payload(Map.of(
                            "reason", historyEvent.getWorkflowExecutionTerminatedEventAttributes()
                                    .getReason(),
                            "eventId", historyEvent.getEventId()))
                    .build();

            case EVENT_TYPE_ACTIVITY_TASK_SCHEDULED -> {
                String activityId = historyEvent.getActivityTaskScheduledEventAttributes()
                        .getActivityId();
                String activityType = historyEvent.getActivityTaskScheduledEventAttributes()
                        .getActivityType().getName();
                yield WorkflowEvent.builder()
                        .eventId(generateEventId(historyEvent))
                        .workflowId(workflowId)
                        .eventType(WorkflowEvent.EventType.TASK_STARTED)
                        .timestamp(timestamp)
                        .taskId(activityId)
                        .taskRefName(activityType)
                        .payload(Map.of(
                                "activityType", activityType,
                                "activityId", activityId,
                                "eventId", historyEvent.getEventId()))
                        .build();
            }

            case EVENT_TYPE_ACTIVITY_TASK_COMPLETED -> {
                long scheduledEventId = historyEvent.getActivityTaskCompletedEventAttributes()
                        .getScheduledEventId();
                yield WorkflowEvent.builder()
                        .eventId(generateEventId(historyEvent))
                        .workflowId(workflowId)
                        .eventType(WorkflowEvent.EventType.TASK_COMPLETED)
                        .timestamp(timestamp)
                        .payload(Map.of(
                                "scheduledEventId", scheduledEventId,
                                "eventId", historyEvent.getEventId()))
                        .build();
            }

            case EVENT_TYPE_ACTIVITY_TASK_FAILED -> {
                long scheduledEventId = historyEvent.getActivityTaskFailedEventAttributes()
                        .getScheduledEventId();
                yield WorkflowEvent.builder()
                        .eventId(generateEventId(historyEvent))
                        .workflowId(workflowId)
                        .eventType(WorkflowEvent.EventType.TASK_FAILED)
                        .timestamp(timestamp)
                        .payload(Map.of(
                                "scheduledEventId", scheduledEventId,
                                "failure", historyEvent.getActivityTaskFailedEventAttributes()
                                        .getFailure().getMessage(),
                                "eventId", historyEvent.getEventId()))
                        .build();
            }

            default -> null; // Skip unrecognized event types
        };
    }

    private String generateEventId(HistoryEvent event) {
        return UUID.randomUUID().toString();
    }

    private long timestampToMillis(Timestamp timestamp) {
        return timestamp.getSeconds() * 1000 + timestamp.getNanos() / 1_000_000;
    }

    private String extractWorkflowId(String taskId) {
        // Try format: workflowId-task-N
        if (taskId.contains("-task-")) {
            return taskId.substring(0, taskId.lastIndexOf("-task-"));
        }
        // Try format: workflowId-N (last segment is numeric)
        int lastDash = taskId.lastIndexOf('-');
        if (lastDash > 0) {
            String suffix = taskId.substring(lastDash + 1);
            try {
                Integer.parseInt(suffix);
                return taskId.substring(0, lastDash);
            } catch (NumberFormatException e) {
                // Not a numeric suffix
            }
        }
        return null;
    }
}
