package io.temporal.conductor.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.temporal.conductor.dto.SearchResult;
import io.temporal.conductor.dto.StartWorkflowRequest;
import io.temporal.conductor.dto.Workflow;
import io.temporal.conductor.dto.WorkflowStatus;
import io.temporal.conductor.dto.WorkflowSummary;
import io.temporal.conductor.service.WorkflowService;
// Validation disabled for stub mode - Conductor's validation requires its own Spring beans
import java.util.List;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API endpoints for workflow operations.
 */
@RestController
@RequestMapping("/api/workflow")
@Tag(name = "Workflow", description = "Workflow execution APIs")
public class WorkflowResource {

    private final WorkflowService workflowService;

    public WorkflowResource(WorkflowService workflowService) {
        this.workflowService = workflowService;
    }

    @PostMapping
    @Operation(summary = "Start a new workflow execution")
    public ResponseEntity<String> startWorkflow(
            @RequestBody StartWorkflowRequest request) {
        String workflowId = workflowService.startWorkflow(request);
        return ResponseEntity.status(org.springframework.http.HttpStatus.CREATED).body(workflowId);
    }

    @PostMapping("/{name}")
    @Operation(summary = "Start a workflow by name with input")
    public ResponseEntity<String> startWorkflowByName(
            @PathVariable String name,
            @RequestParam(required = false) @Parameter(description = "Workflow version")
            Integer version,
            @RequestParam(required = false) @Parameter(description = "Correlation ID")
            String correlationId,
            @RequestParam(required = false) @Parameter(description = "Priority")
            Integer priority,
            @RequestBody Map<String, Object> input) {
        StartWorkflowRequest request = new StartWorkflowRequest();
        request.setName(name);
        request.setVersion(version);
        request.setCorrelationId(correlationId);
        request.setPriority(priority);
        request.setInput(input);
        String workflowId = workflowService.startWorkflow(request);
        return ResponseEntity.status(org.springframework.http.HttpStatus.CREATED).body(workflowId);
    }

    @GetMapping("/{workflowId}")
    @Operation(summary = "Get workflow execution by ID")
    public ResponseEntity<Workflow> getWorkflow(
            @PathVariable String workflowId,
            @RequestParam(defaultValue = "true")
            @Parameter(description = "Include tasks in response")
            boolean includeTasks) {
        Workflow workflow = workflowService.getWorkflow(workflowId, includeTasks);
        return ResponseEntity.ok(workflow);
    }

    @GetMapping("/{workflowId}/status")
    @Operation(summary = "Get workflow status only")
    public ResponseEntity<WorkflowStatus> getWorkflowStatus(@PathVariable String workflowId) {
        WorkflowStatus status = workflowService.getWorkflowStatus(workflowId);
        return ResponseEntity.ok(status);
    }

    @DeleteMapping("/{workflowId}")
    @Operation(summary = "Terminate a running workflow")
    public ResponseEntity<Void> terminateWorkflow(
            @PathVariable String workflowId,
            @RequestParam(required = false) @Parameter(description = "Termination reason")
            String reason) {
        workflowService.terminateWorkflow(workflowId, reason);
        return ResponseEntity.noContent().build();
    }

    @PutMapping("/{workflowId}/pause")
    @Operation(summary = "Pause a running workflow")
    public ResponseEntity<Void> pauseWorkflow(@PathVariable String workflowId) {
        workflowService.pauseWorkflow(workflowId);
        return ResponseEntity.ok().build();
    }

    @PutMapping("/{workflowId}/resume")
    @Operation(summary = "Resume a paused workflow")
    public ResponseEntity<Void> resumeWorkflow(@PathVariable String workflowId) {
        workflowService.resumeWorkflow(workflowId);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/{workflowId}/restart")
    @Operation(summary = "Restart a workflow from the beginning")
    public ResponseEntity<String> restartWorkflow(
            @PathVariable String workflowId,
            @RequestParam(defaultValue = "false")
            @Parameter(description = "Use latest workflow definitions")
            boolean useLatestDefinitions) {
        String newWorkflowId = workflowService.restartWorkflow(workflowId, useLatestDefinitions);
        return ResponseEntity.ok(newWorkflowId);
    }

    @PostMapping("/{workflowId}/retry")
    @Operation(summary = "Retry a failed workflow from the failed task")
    public ResponseEntity<Void> retryWorkflow(
            @PathVariable String workflowId,
            @RequestParam(defaultValue = "false")
            @Parameter(description = "Resume subworkflow tasks")
            boolean resumeSubworkflowTasks) {
        workflowService.retryWorkflow(workflowId, resumeSubworkflowTasks);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/running/{name}")
    @Operation(summary = "Get running workflow IDs by name")
    public ResponseEntity<List<String>> getRunningWorkflows(
            @PathVariable String name,
            @RequestParam(required = false) @Parameter(description = "Workflow version")
            Integer version,
            @RequestParam(required = false) @Parameter(description = "Start time filter")
            Long startTime,
            @RequestParam(required = false) @Parameter(description = "End time filter")
            Long endTime) {
        List<String> workflowIds = workflowService.getRunningWorkflows(
                name, version, startTime, endTime);
        return ResponseEntity.ok(workflowIds);
    }

    @GetMapping("/search")
    @Operation(summary = "Search for workflows")
    public ResponseEntity<SearchResult<WorkflowSummary>> searchWorkflows(
            @RequestParam(defaultValue = "0") @Parameter(description = "Start index") int start,
            @RequestParam(defaultValue = "100") @Parameter(description = "Page size") int size,
            @RequestParam(required = false) @Parameter(description = "Sort field") String sort,
            @RequestParam(required = false) @Parameter(description = "Free text search")
            String freeText,
            @RequestParam(required = false) @Parameter(description = "Search query") String query) {
        SearchResult<WorkflowSummary> result = workflowService.searchWorkflows(
                start, size, sort, freeText, query);
        return ResponseEntity.ok(result);
    }

    @GetMapping("/search-v2")
    @Operation(summary = "Search for workflows (v2 with full workflow objects)")
    public ResponseEntity<SearchResult<Workflow>> searchWorkflowsV2(
            @RequestParam(defaultValue = "0") @Parameter(description = "Start index") int start,
            @RequestParam(defaultValue = "100") @Parameter(description = "Page size") int size,
            @RequestParam(required = false) @Parameter(description = "Sort field") String sort,
            @RequestParam(required = false) @Parameter(description = "Free text search")
            String freeText,
            @RequestParam(required = false) @Parameter(description = "Search query") String query) {
        SearchResult<Workflow> result = workflowService.searchWorkflowsV2(
                start, size, sort, freeText, query);
        return ResponseEntity.ok(result);
    }
}
