package io.temporal.conductor.api;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.temporal.conductor.dto.SearchResult;
import io.temporal.conductor.dto.TaskSummary;
import io.temporal.conductor.dto.UpdateTaskRequest;
import io.temporal.conductor.service.TaskService;
import java.util.List;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST API endpoints for task operations.
 */
@RestController
@RequestMapping("/api/tasks")
@Tag(name = "Task", description = "Task execution APIs")
public class TaskResource {

    private final TaskService taskService;

    public TaskResource(TaskService taskService) {
        this.taskService = taskService;
    }

    @GetMapping("/{taskId}")
    @Operation(summary = "Get task by ID")
    public ResponseEntity<Task> getTask(@PathVariable String taskId) {
        return ResponseEntity.ok(taskService.getTask(taskId));
    }

    @GetMapping("/search")
    @Operation(summary = "Search for tasks")
    public ResponseEntity<SearchResult<TaskSummary>> searchTasks(
            @RequestParam(defaultValue = "0") @Parameter(description = "Start index") int start,
            @RequestParam(defaultValue = "100") @Parameter(description = "Page size") int size,
            @RequestParam(required = false) @Parameter(description = "Sort field") String sort,
            @RequestParam(required = false) @Parameter(description = "Free text search")
            String freeText,
            @RequestParam(required = false) @Parameter(description = "Search query") String query) {
        return ResponseEntity.ok(taskService.searchTasks(start, size, sort, freeText, query));
    }

    @PostMapping("/{taskId}/log")
    @Operation(summary = "Add a log entry to a task")
    public ResponseEntity<Void> addTaskLog(
            @PathVariable String taskId,
            @RequestBody String log) {
        taskService.addTaskLog(taskId, log);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/{taskId}/log")
    @Operation(summary = "Get task execution logs")
    public ResponseEntity<List<TaskExecLog>> getTaskLogs(@PathVariable String taskId) {
        return ResponseEntity.ok(taskService.getTaskLogs(taskId));
    }

    @PostMapping("/queue/poll/{taskType}")
    @Operation(summary = "Poll for a task of the specified type")
    public ResponseEntity<Task> pollTask(
            @PathVariable String taskType,
            @RequestParam(required = false) @Parameter(description = "Worker ID") String workerId,
            @RequestParam(required = false) @Parameter(description = "Task domain") String domain) {
        Task task = taskService.poll(taskType, workerId, domain);
        return task != null ? ResponseEntity.ok(task) : ResponseEntity.noContent().build();
    }

    @PostMapping
    @Operation(summary = "Update task status")
    public ResponseEntity<String> updateTask(@RequestBody UpdateTaskRequest request) {
        String taskId = taskService.updateTask(request.toTaskResult());
        return ResponseEntity.ok(taskId);
    }
}
