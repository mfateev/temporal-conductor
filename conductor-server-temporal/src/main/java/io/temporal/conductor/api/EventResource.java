package io.temporal.conductor.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.temporal.conductor.dto.WorkflowEvent;
import io.temporal.conductor.service.EventService;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API endpoints for workflow and task events.
 */
@RestController
@RequestMapping("/api/events")
@Tag(name = "Events", description = "Workflow and task lifecycle event APIs")
public class EventResource {

    private final EventService eventService;

    public EventResource(EventService eventService) {
        this.eventService = eventService;
    }

    @GetMapping("/workflow/{workflowId}")
    @Operation(summary = "Get workflow lifecycle events")
    public ResponseEntity<List<WorkflowEvent>> getWorkflowEvents(
            @PathVariable @Parameter(description = "Workflow ID") String workflowId,
            @RequestParam(defaultValue = "0") @Parameter(description = "Start index") int start,
            @RequestParam(defaultValue = "100") @Parameter(description = "Page size") int size) {
        List<WorkflowEvent> events = eventService.getWorkflowEvents(workflowId, start, size);
        return ResponseEntity.ok(events);
    }

    @GetMapping("/task/{taskId}")
    @Operation(summary = "Get task events")
    public ResponseEntity<List<WorkflowEvent>> getTaskEvents(
            @PathVariable @Parameter(description = "Task ID") String taskId) {
        List<WorkflowEvent> events = eventService.getTaskEvents(taskId);
        return ResponseEntity.ok(events);
    }
}
