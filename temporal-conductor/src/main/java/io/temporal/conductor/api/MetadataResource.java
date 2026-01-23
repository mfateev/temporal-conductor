package io.temporal.conductor.api;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.temporal.conductor.service.MetadataService;
// Validation disabled for stub mode - Conductor's validation requires its own Spring beans
import java.util.List;
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
 * REST API endpoints for metadata operations (workflow and task definitions).
 */
@RestController
@RequestMapping("/api/metadata")
@Tag(name = "Metadata", description = "Workflow and Task definition APIs")
public class MetadataResource {

    private final MetadataService metadataService;

    public MetadataResource(MetadataService metadataService) {
        this.metadataService = metadataService;
    }

    // Workflow Definitions

    @GetMapping("/workflow")
    @Operation(summary = "Get all workflow definitions")
    public ResponseEntity<List<WorkflowDef>> getAllWorkflowDefs() {
        return ResponseEntity.ok(metadataService.getAllWorkflowDefs());
    }

    @GetMapping("/workflow/{name}")
    @Operation(summary = "Get workflow definition by name")
    public ResponseEntity<WorkflowDef> getWorkflowDef(
            @PathVariable String name,
            @RequestParam(required = false) @Parameter(description = "Workflow version")
            Integer version) {
        WorkflowDef def;
        if (version != null) {
            def = metadataService.getWorkflowDef(name, version);
        } else {
            def = metadataService.getLatestWorkflowDef(name);
        }
        return ResponseEntity.ok(def);
    }

    @PostMapping("/workflow")
    @Operation(summary = "Create a new workflow definition")
    public ResponseEntity<Void> createWorkflowDef(
            @RequestBody WorkflowDef workflowDef) {
        metadataService.registerWorkflowDef(workflowDef);
        return ResponseEntity.noContent().build();
    }

    @PutMapping("/workflow")
    @Operation(summary = "Update workflow definitions")
    public ResponseEntity<Void> updateWorkflowDef(
            @RequestBody List<WorkflowDef> workflowDefs) {
        metadataService.updateWorkflowDefs(workflowDefs);
        return ResponseEntity.noContent().build();
    }

    @DeleteMapping("/workflow/{name}/{version}")
    @Operation(summary = "Delete a workflow definition")
    public ResponseEntity<Void> deleteWorkflowDef(
            @PathVariable String name,
            @PathVariable int version) {
        metadataService.deleteWorkflowDef(name, version);
        return ResponseEntity.noContent().build();
    }

    // Task Definitions

    @GetMapping("/taskdefs")
    @Operation(summary = "Get all task definitions")
    public ResponseEntity<List<TaskDef>> getAllTaskDefs() {
        return ResponseEntity.ok(metadataService.getAllTaskDefs());
    }

    @GetMapping("/taskdefs/{taskType}")
    @Operation(summary = "Get task definition by type")
    public ResponseEntity<TaskDef> getTaskDef(@PathVariable String taskType) {
        return ResponseEntity.ok(metadataService.getTaskDef(taskType));
    }

    @PostMapping("/taskdefs")
    @Operation(summary = "Register task definitions")
    public ResponseEntity<Void> registerTaskDefs(
            @RequestBody List<TaskDef> taskDefs) {
        metadataService.registerTaskDefs(taskDefs);
        return ResponseEntity.noContent().build();
    }

    @DeleteMapping("/taskdefs/{taskType}")
    @Operation(summary = "Delete a task definition")
    public ResponseEntity<Void> deleteTaskDef(@PathVariable String taskType) {
        metadataService.deleteTaskDef(taskType);
        return ResponseEntity.noContent().build();
    }
}
