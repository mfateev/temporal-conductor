package io.temporal.conductor.service.stub;

import io.temporal.conductor.dto.WorkflowEvent;
import io.temporal.conductor.dto.WorkflowEvent.EventType;
import io.temporal.conductor.service.EventService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

/**
 * Stub implementation of EventService for testing and UI validation.
 */
@Service
@Profile("stub")
public class StubEventService implements EventService {

    private final Map<String, List<WorkflowEvent>> workflowEvents = new ConcurrentHashMap<>();
    private final Map<String, List<WorkflowEvent>> taskEvents = new ConcurrentHashMap<>();

    @Override
    public List<WorkflowEvent> getWorkflowEvents(String workflowId, int start, int size) {
        List<WorkflowEvent> events = workflowEvents.get(workflowId);
        if (events == null) {
            // Generate mock events for testing
            events = generateMockWorkflowEvents(workflowId);
            workflowEvents.put(workflowId, events);
        }

        // Apply pagination
        if (start >= events.size()) {
            return Collections.emptyList();
        }
        int end = Math.min(start + size, events.size());
        return new ArrayList<>(events.subList(start, end));
    }

    @Override
    public List<WorkflowEvent> getTaskEvents(String taskId) {
        List<WorkflowEvent> events = taskEvents.get(taskId);
        if (events == null) {
            // Generate mock events for testing
            events = generateMockTaskEvents(taskId);
            taskEvents.put(taskId, events);
        }
        return new ArrayList<>(events);
    }

    @Override
    public void recordEvent(WorkflowEvent event) {
        // Store in workflow events
        workflowEvents.computeIfAbsent(event.getWorkflowId(), k -> new ArrayList<>())
                .add(event);

        // Store in task events if applicable
        if (event.getTaskId() != null) {
            taskEvents.computeIfAbsent(event.getTaskId(), k -> new ArrayList<>())
                    .add(event);
        }
    }

    private List<WorkflowEvent> generateMockWorkflowEvents(String workflowId) {
        List<WorkflowEvent> events = new ArrayList<>();
        long baseTime = System.currentTimeMillis() - 60000; // 1 minute ago

        // Workflow started event
        events.add(WorkflowEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .workflowId(workflowId)
                .eventType(EventType.STARTED)
                .timestamp(baseTime)
                .payload(Map.of("workflowType", "sample-workflow", "version", 1))
                .build());

        // Task started event
        String taskId1 = workflowId + "-task-1";
        events.add(WorkflowEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .workflowId(workflowId)
                .eventType(EventType.TASK_STARTED)
                .timestamp(baseTime + 1000)
                .taskId(taskId1)
                .taskRefName("task1_ref")
                .payload(Map.of("taskType", "SIMPLE", "taskDefName", "simple_task_1"))
                .build());

        // Task completed event
        events.add(WorkflowEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .workflowId(workflowId)
                .eventType(EventType.TASK_COMPLETED)
                .timestamp(baseTime + 5000)
                .taskId(taskId1)
                .taskRefName("task1_ref")
                .payload(Map.of("taskType", "SIMPLE", "taskDefName", "simple_task_1"))
                .build());

        // Task 2 started event
        String taskId2 = workflowId + "-task-2";
        events.add(WorkflowEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .workflowId(workflowId)
                .eventType(EventType.TASK_STARTED)
                .timestamp(baseTime + 6000)
                .taskId(taskId2)
                .taskRefName("task2_ref")
                .payload(Map.of("taskType", "SIMPLE", "taskDefName", "simple_task_2"))
                .build());

        // Task 2 completed event
        events.add(WorkflowEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .workflowId(workflowId)
                .eventType(EventType.TASK_COMPLETED)
                .timestamp(baseTime + 10000)
                .taskId(taskId2)
                .taskRefName("task2_ref")
                .payload(Map.of("taskType", "SIMPLE", "taskDefName", "simple_task_2"))
                .build());

        // Workflow completed event
        events.add(WorkflowEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .workflowId(workflowId)
                .eventType(EventType.COMPLETED)
                .timestamp(baseTime + 11000)
                .payload(Map.of("output", Map.of("result", "success")))
                .build());

        return events;
    }

    private List<WorkflowEvent> generateMockTaskEvents(String taskId) {
        List<WorkflowEvent> events = new ArrayList<>();
        long baseTime = System.currentTimeMillis() - 30000; // 30 seconds ago

        // Extract workflow ID from task ID (assuming format: workflowId-task-N)
        String workflowId = taskId.contains("-task-")
                ? taskId.substring(0, taskId.lastIndexOf("-task-"))
                : "unknown-workflow";

        // Task started event
        events.add(WorkflowEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .workflowId(workflowId)
                .eventType(EventType.TASK_STARTED)
                .timestamp(baseTime)
                .taskId(taskId)
                .taskRefName("task_ref")
                .payload(Map.of("taskType", "SIMPLE", "taskDefName", "simple_task"))
                .build());

        // Task completed event
        events.add(WorkflowEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .workflowId(workflowId)
                .eventType(EventType.TASK_COMPLETED)
                .timestamp(baseTime + 5000)
                .taskId(taskId)
                .taskRefName("task_ref")
                .payload(Map.of("taskType", "SIMPLE", "taskDefName", "simple_task",
                        "output", Map.of("result", "task completed")))
                .build());

        return events;
    }
}
