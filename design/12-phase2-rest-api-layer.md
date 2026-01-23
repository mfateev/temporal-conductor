# Phase 2: REST API Layer (with Stub Backend)

## Overview

This phase implements Conductor-compatible REST endpoints with stub/mock implementations to enable early UI validation before building the full Temporal backend.

**Goal**: Validate that the Conductor UI works with our API before investing in complex backend implementation.

**Language**: Java (Spring Boot)

## Project Structure

Extend the `conductor-server-temporal` module:

```
conductor-server-temporal/
├── src/main/java/io/temporal/conductor/
│   ├── workflow/                    # From Phase 1
│   ├── activity/                    # From Phase 1
│   ├── executor/                    # From Phase 1
│   ├── api/                         # NEW: REST Controllers
│   │   ├── WorkflowResource.java
│   │   ├── MetadataResource.java
│   │   ├── TaskResource.java
│   │   └── EventResource.java
│   ├── service/                     # NEW: Service Layer
│   │   ├── WorkflowService.java
│   │   ├── MetadataService.java
│   │   ├── TaskService.java
│   │   └── stub/
│   │       ├── StubWorkflowService.java
│   │       ├── StubMetadataService.java
│   │       └── StubDataGenerator.java
│   ├── dto/                         # NEW: REST DTOs
│   │   ├── StartWorkflowRequest.java
│   │   ├── WorkflowSummary.java
│   │   ├── SearchResult.java
│   │   ├── BulkResponse.java
│   │   └── ...
│   └── config/                      # NEW: Spring Configuration
│       ├── WebConfig.java
│       └── ServiceConfig.java
├── src/main/resources/
│   └── application.yml
└── src/test/java/io/temporal/conductor/
    └── api/                         # NEW: API Tests
        ├── WorkflowResourceTest.java
        ├── MetadataResourceTest.java
        └── TaskResourceTest.java
```

---

## 1. Dependencies

Add to `build.gradle`:

```groovy
plugins {
    id 'java'
    id 'org.springframework.boot' version '3.2.2'
    id 'io.spring.dependency-management' version '1.1.4'
    id 'checkstyle'
}

dependencies {
    // Spring Boot
    implementation 'org.springframework.boot:spring-boot-starter-web'
    implementation 'org.springframework.boot:spring-boot-starter-validation'

    // OpenAPI documentation
    implementation 'org.springdoc:springdoc-openapi-starter-webmvc-ui:2.3.0'

    // Existing dependencies from Phase 1
    implementation 'io.temporal:temporal-sdk:1.25.0'
    implementation 'org.conductoross:conductor-core:3.21.23'
    implementation 'org.conductoross:conductor-common:3.21.23'
    implementation 'com.fasterxml.jackson.core:jackson-databind:2.17.0'

    // Testing
    testImplementation 'org.springframework.boot:spring-boot-starter-test'
    testImplementation 'io.temporal:temporal-testing:1.25.0'
}
```

---

## 2. REST Endpoints

### 2.1 Workflow Endpoints (`WorkflowResource.java`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/workflow` | Start a new workflow |
| POST | `/api/workflow/{name}` | Start workflow by name |
| GET | `/api/workflow/{workflowId}` | Get workflow by ID |
| GET | `/api/workflow/{workflowId}/status` | Get workflow status only |
| DELETE | `/api/workflow/{workflowId}` | Terminate workflow |
| PUT | `/api/workflow/{workflowId}/pause` | Pause workflow |
| PUT | `/api/workflow/{workflowId}/resume` | Resume workflow |
| POST | `/api/workflow/{workflowId}/restart` | Restart workflow |
| POST | `/api/workflow/{workflowId}/retry` | Retry from failed task |
| POST | `/api/workflow/{workflowId}/rerun` | Rerun workflow |
| GET | `/api/workflow/running/{name}` | Get running workflows by name |
| GET | `/api/workflow/search` | Search workflows |
| GET | `/api/workflow/search-v2` | Search workflows (v2) |

```java
package io.temporal.conductor.api;

import io.temporal.conductor.dto.*;
import io.temporal.conductor.service.WorkflowService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/workflow")
public class WorkflowResource {

    private final WorkflowService workflowService;

    public WorkflowResource(WorkflowService workflowService) {
        this.workflowService = workflowService;
    }

    @PostMapping
    public ResponseEntity<String> startWorkflow(@Valid @RequestBody StartWorkflowRequest request) {
        String workflowId = workflowService.startWorkflow(request);
        return ResponseEntity.ok(workflowId);
    }

    @PostMapping("/{name}")
    public ResponseEntity<String> startWorkflowByName(
            @PathVariable String name,
            @RequestParam(required = false) Integer version,
            @RequestParam(required = false) String correlationId,
            @RequestParam(required = false) Integer priority,
            @RequestBody Map<String, Object> input) {
        StartWorkflowRequest request = new StartWorkflowRequest();
        request.setName(name);
        request.setVersion(version);
        request.setCorrelationId(correlationId);
        request.setPriority(priority);
        request.setInput(input);
        String workflowId = workflowService.startWorkflow(request);
        return ResponseEntity.ok(workflowId);
    }

    @GetMapping("/{workflowId}")
    public ResponseEntity<Workflow> getWorkflow(
            @PathVariable String workflowId,
            @RequestParam(defaultValue = "true") boolean includeTasks) {
        Workflow workflow = workflowService.getWorkflow(workflowId, includeTasks);
        return ResponseEntity.ok(workflow);
    }

    @GetMapping("/{workflowId}/status")
    public ResponseEntity<WorkflowStatus> getWorkflowStatus(@PathVariable String workflowId) {
        WorkflowStatus status = workflowService.getWorkflowStatus(workflowId);
        return ResponseEntity.ok(status);
    }

    @DeleteMapping("/{workflowId}")
    public ResponseEntity<Void> terminateWorkflow(
            @PathVariable String workflowId,
            @RequestParam(required = false) String reason) {
        workflowService.terminateWorkflow(workflowId, reason);
        return ResponseEntity.noContent().build();
    }

    @PutMapping("/{workflowId}/pause")
    public ResponseEntity<Void> pauseWorkflow(@PathVariable String workflowId) {
        workflowService.pauseWorkflow(workflowId);
        return ResponseEntity.ok().build();
    }

    @PutMapping("/{workflowId}/resume")
    public ResponseEntity<Void> resumeWorkflow(@PathVariable String workflowId) {
        workflowService.resumeWorkflow(workflowId);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/{workflowId}/restart")
    public ResponseEntity<String> restartWorkflow(
            @PathVariable String workflowId,
            @RequestParam(defaultValue = "false") boolean useLatestDefinitions) {
        String newWorkflowId = workflowService.restartWorkflow(workflowId, useLatestDefinitions);
        return ResponseEntity.ok(newWorkflowId);
    }

    @PostMapping("/{workflowId}/retry")
    public ResponseEntity<Void> retryWorkflow(
            @PathVariable String workflowId,
            @RequestParam(defaultValue = "false") boolean resumeSubworkflowTasks) {
        workflowService.retryWorkflow(workflowId, resumeSubworkflowTasks);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/running/{name}")
    public ResponseEntity<List<String>> getRunningWorkflows(
            @PathVariable String name,
            @RequestParam(required = false) Integer version,
            @RequestParam(required = false) Long startTime,
            @RequestParam(required = false) Long endTime) {
        List<String> workflowIds = workflowService.getRunningWorkflows(name, version, startTime, endTime);
        return ResponseEntity.ok(workflowIds);
    }

    @GetMapping("/search")
    public ResponseEntity<SearchResult<WorkflowSummary>> searchWorkflows(
            @RequestParam(defaultValue = "0") int start,
            @RequestParam(defaultValue = "100") int size,
            @RequestParam(required = false) String sort,
            @RequestParam(required = false) String freeText,
            @RequestParam(required = false) String query) {
        SearchResult<WorkflowSummary> result = workflowService.searchWorkflows(start, size, sort, freeText, query);
        return ResponseEntity.ok(result);
    }

    @GetMapping("/search-v2")
    public ResponseEntity<SearchResult<Workflow>> searchWorkflowsV2(
            @RequestParam(defaultValue = "0") int start,
            @RequestParam(defaultValue = "100") int size,
            @RequestParam(required = false) String sort,
            @RequestParam(required = false) String freeText,
            @RequestParam(required = false) String query) {
        SearchResult<Workflow> result = workflowService.searchWorkflowsV2(start, size, sort, freeText, query);
        return ResponseEntity.ok(result);
    }
}
```

### 2.2 Metadata Endpoints (`MetadataResource.java`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/metadata/workflow` | Get all workflow definitions |
| GET | `/api/metadata/workflow/{name}` | Get latest workflow definition |
| GET | `/api/metadata/workflow/{name}/{version}` | Get specific version |
| POST | `/api/metadata/workflow` | Create workflow definition |
| PUT | `/api/metadata/workflow` | Update workflow definition |
| DELETE | `/api/metadata/workflow/{name}/{version}` | Delete workflow definition |
| GET | `/api/metadata/taskdefs` | Get all task definitions |
| GET | `/api/metadata/taskdefs/{taskType}` | Get task definition |
| POST | `/api/metadata/taskdefs` | Register task definitions |
| DELETE | `/api/metadata/taskdefs/{taskType}` | Delete task definition |

```java
package io.temporal.conductor.api;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import io.temporal.conductor.service.MetadataService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/metadata")
public class MetadataResource {

    private final MetadataService metadataService;

    public MetadataResource(MetadataService metadataService) {
        this.metadataService = metadataService;
    }

    // Workflow Definitions

    @GetMapping("/workflow")
    public ResponseEntity<List<WorkflowDef>> getAllWorkflowDefs() {
        return ResponseEntity.ok(metadataService.getAllWorkflowDefs());
    }

    @GetMapping("/workflow/{name}")
    public ResponseEntity<WorkflowDef> getWorkflowDef(
            @PathVariable String name,
            @RequestParam(required = false) Integer version) {
        WorkflowDef def = version != null
            ? metadataService.getWorkflowDef(name, version)
            : metadataService.getLatestWorkflowDef(name);
        return ResponseEntity.ok(def);
    }

    @PostMapping("/workflow")
    public ResponseEntity<Void> createWorkflowDef(@Valid @RequestBody WorkflowDef workflowDef) {
        metadataService.registerWorkflowDef(workflowDef);
        return ResponseEntity.noContent().build();
    }

    @PutMapping("/workflow")
    public ResponseEntity<Void> updateWorkflowDef(@Valid @RequestBody List<WorkflowDef> workflowDefs) {
        metadataService.updateWorkflowDefs(workflowDefs);
        return ResponseEntity.noContent().build();
    }

    @DeleteMapping("/workflow/{name}/{version}")
    public ResponseEntity<Void> deleteWorkflowDef(
            @PathVariable String name,
            @PathVariable int version) {
        metadataService.deleteWorkflowDef(name, version);
        return ResponseEntity.noContent().build();
    }

    // Task Definitions

    @GetMapping("/taskdefs")
    public ResponseEntity<List<TaskDef>> getAllTaskDefs() {
        return ResponseEntity.ok(metadataService.getAllTaskDefs());
    }

    @GetMapping("/taskdefs/{taskType}")
    public ResponseEntity<TaskDef> getTaskDef(@PathVariable String taskType) {
        return ResponseEntity.ok(metadataService.getTaskDef(taskType));
    }

    @PostMapping("/taskdefs")
    public ResponseEntity<Void> registerTaskDefs(@Valid @RequestBody List<TaskDef> taskDefs) {
        metadataService.registerTaskDefs(taskDefs);
        return ResponseEntity.noContent().build();
    }

    @DeleteMapping("/taskdefs/{taskType}")
    public ResponseEntity<Void> deleteTaskDef(@PathVariable String taskType) {
        metadataService.deleteTaskDef(taskType);
        return ResponseEntity.noContent().build();
    }
}
```

### 2.3 Task Endpoints (`TaskResource.java`)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/tasks/{taskId}` | Get task by ID |
| GET | `/api/tasks/search` | Search tasks |
| POST | `/api/tasks/{taskId}/log` | Add task log |
| GET | `/api/tasks/{taskId}/log` | Get task logs |
| POST | `/api/tasks/queue/poll/{taskType}` | Poll for task |
| POST | `/api/tasks` | Update task |

```java
package io.temporal.conductor.api;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.metadata.tasks.TaskResult;
import io.temporal.conductor.dto.SearchResult;
import io.temporal.conductor.dto.TaskSummary;
import io.temporal.conductor.service.TaskService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/tasks")
public class TaskResource {

    private final TaskService taskService;

    public TaskResource(TaskService taskService) {
        this.taskService = taskService;
    }

    @GetMapping("/{taskId}")
    public ResponseEntity<Task> getTask(@PathVariable String taskId) {
        return ResponseEntity.ok(taskService.getTask(taskId));
    }

    @GetMapping("/search")
    public ResponseEntity<SearchResult<TaskSummary>> searchTasks(
            @RequestParam(defaultValue = "0") int start,
            @RequestParam(defaultValue = "100") int size,
            @RequestParam(required = false) String sort,
            @RequestParam(required = false) String freeText,
            @RequestParam(required = false) String query) {
        return ResponseEntity.ok(taskService.searchTasks(start, size, sort, freeText, query));
    }

    @PostMapping("/{taskId}/log")
    public ResponseEntity<Void> addTaskLog(
            @PathVariable String taskId,
            @RequestBody String log) {
        taskService.addTaskLog(taskId, log);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/{taskId}/log")
    public ResponseEntity<List<TaskExecLog>> getTaskLogs(@PathVariable String taskId) {
        return ResponseEntity.ok(taskService.getTaskLogs(taskId));
    }

    @PostMapping("/queue/poll/{taskType}")
    public ResponseEntity<Task> pollTask(
            @PathVariable String taskType,
            @RequestParam(required = false) String workerId,
            @RequestParam(required = false) String domain) {
        Task task = taskService.poll(taskType, workerId, domain);
        return task != null ? ResponseEntity.ok(task) : ResponseEntity.noContent().build();
    }

    @PostMapping
    public ResponseEntity<String> updateTask(@RequestBody TaskResult taskResult) {
        String taskId = taskService.updateTask(taskResult);
        return ResponseEntity.ok(taskId);
    }
}
```

---

## 3. DTOs

### 3.1 StartWorkflowRequest.java

```java
package io.temporal.conductor.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class StartWorkflowRequest {
    private String name;
    private Integer version;
    private String correlationId;
    private Map<String, Object> input;
    private Map<String, String> taskToDomain;
    private WorkflowDef workflowDef;  // Inline definition
    private String externalInputPayloadStoragePath;
    private Integer priority;
    private String createdBy;

    // Getters and setters
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public Integer getVersion() { return version; }
    public void setVersion(Integer version) { this.version = version; }
    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    public Map<String, Object> getInput() { return input; }
    public void setInput(Map<String, Object> input) { this.input = input; }
    public Map<String, String> getTaskToDomain() { return taskToDomain; }
    public void setTaskToDomain(Map<String, String> taskToDomain) { this.taskToDomain = taskToDomain; }
    public WorkflowDef getWorkflowDef() { return workflowDef; }
    public void setWorkflowDef(WorkflowDef workflowDef) { this.workflowDef = workflowDef; }
    public String getExternalInputPayloadStoragePath() { return externalInputPayloadStoragePath; }
    public void setExternalInputPayloadStoragePath(String path) { this.externalInputPayloadStoragePath = path; }
    public Integer getPriority() { return priority; }
    public void setPriority(Integer priority) { this.priority = priority; }
    public String getCreatedBy() { return createdBy; }
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }
}
```

### 3.2 SearchResult.java

```java
package io.temporal.conductor.dto;

import java.util.List;

public class SearchResult<T> {
    private long totalHits;
    private List<T> results;

    public SearchResult() {}

    public SearchResult(long totalHits, List<T> results) {
        this.totalHits = totalHits;
        this.results = results;
    }

    public long getTotalHits() { return totalHits; }
    public void setTotalHits(long totalHits) { this.totalHits = totalHits; }
    public List<T> getResults() { return results; }
    public void setResults(List<T> results) { this.results = results; }
}
```

### 3.3 WorkflowSummary.java

```java
package io.temporal.conductor.dto;

import java.util.Set;

public class WorkflowSummary {
    private String workflowType;
    private int version;
    private String workflowId;
    private String correlationId;
    private String startTime;
    private String updateTime;
    private String endTime;
    private String status;
    private String input;
    private String output;
    private String reasonForIncompletion;
    private long executionTime;
    private String event;
    private String failedReferenceTaskNames;
    private String externalInputPayloadStoragePath;
    private String externalOutputPayloadStoragePath;
    private int priority;
    private Set<String> failedTaskNames;

    // Getters and setters for all fields
    public String getWorkflowType() { return workflowType; }
    public void setWorkflowType(String workflowType) { this.workflowType = workflowType; }
    public int getVersion() { return version; }
    public void setVersion(int version) { this.version = version; }
    public String getWorkflowId() { return workflowId; }
    public void setWorkflowId(String workflowId) { this.workflowId = workflowId; }
    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    public String getStartTime() { return startTime; }
    public void setStartTime(String startTime) { this.startTime = startTime; }
    public String getUpdateTime() { return updateTime; }
    public void setUpdateTime(String updateTime) { this.updateTime = updateTime; }
    public String getEndTime() { return endTime; }
    public void setEndTime(String endTime) { this.endTime = endTime; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getInput() { return input; }
    public void setInput(String input) { this.input = input; }
    public String getOutput() { return output; }
    public void setOutput(String output) { this.output = output; }
    public String getReasonForIncompletion() { return reasonForIncompletion; }
    public void setReasonForIncompletion(String reason) { this.reasonForIncompletion = reason; }
    public long getExecutionTime() { return executionTime; }
    public void setExecutionTime(long executionTime) { this.executionTime = executionTime; }
    public String getEvent() { return event; }
    public void setEvent(String event) { this.event = event; }
    public String getFailedReferenceTaskNames() { return failedReferenceTaskNames; }
    public void setFailedReferenceTaskNames(String names) { this.failedReferenceTaskNames = names; }
    public String getExternalInputPayloadStoragePath() { return externalInputPayloadStoragePath; }
    public void setExternalInputPayloadStoragePath(String path) { this.externalInputPayloadStoragePath = path; }
    public String getExternalOutputPayloadStoragePath() { return externalOutputPayloadStoragePath; }
    public void setExternalOutputPayloadStoragePath(String path) { this.externalOutputPayloadStoragePath = path; }
    public int getPriority() { return priority; }
    public void setPriority(int priority) { this.priority = priority; }
    public Set<String> getFailedTaskNames() { return failedTaskNames; }
    public void setFailedTaskNames(Set<String> failedTaskNames) { this.failedTaskNames = failedTaskNames; }
}
```

### 3.4 Workflow.java (Full workflow with tasks)

```java
package io.temporal.conductor.dto;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Workflow {
    private String workflowId;
    private String parentWorkflowId;
    private String parentWorkflowTaskId;
    private String correlationId;
    private String workflowType;
    private int version;
    private String status;
    private String endTime;
    private long workflowVersion;
    private WorkflowDef workflowDefinition;
    private List<Task> tasks;
    private Map<String, Object> input;
    private Map<String, Object> output;
    private String externalInputPayloadStoragePath;
    private String externalOutputPayloadStoragePath;
    private Map<String, Object> variables;
    private String lastRetriedTime;
    private Set<String> failedReferenceTaskNames;
    private String ownerApp;
    private String createTime;
    private String updateTime;
    private String createdBy;
    private String updatedBy;
    private String failedTaskId;
    private int priority;
    private String reasonForIncompletion;
    private String event;
    private String taskToDomain;

    // Getters and setters - follow same pattern
    public String getWorkflowId() { return workflowId; }
    public void setWorkflowId(String workflowId) { this.workflowId = workflowId; }
    public String getWorkflowType() { return workflowType; }
    public void setWorkflowType(String workflowType) { this.workflowType = workflowType; }
    public int getVersion() { return version; }
    public void setVersion(int version) { this.version = version; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public List<Task> getTasks() { return tasks; }
    public void setTasks(List<Task> tasks) { this.tasks = tasks; }
    public Map<String, Object> getInput() { return input; }
    public void setInput(Map<String, Object> input) { this.input = input; }
    public Map<String, Object> getOutput() { return output; }
    public void setOutput(Map<String, Object> output) { this.output = output; }
    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    public String getReasonForIncompletion() { return reasonForIncompletion; }
    public void setReasonForIncompletion(String reason) { this.reasonForIncompletion = reason; }
    public int getPriority() { return priority; }
    public void setPriority(int priority) { this.priority = priority; }
    public WorkflowDef getWorkflowDefinition() { return workflowDefinition; }
    public void setWorkflowDefinition(WorkflowDef def) { this.workflowDefinition = def; }
    public Map<String, Object> getVariables() { return variables; }
    public void setVariables(Map<String, Object> variables) { this.variables = variables; }
    public String getCreateTime() { return createTime; }
    public void setCreateTime(String createTime) { this.createTime = createTime; }
    public String getUpdateTime() { return updateTime; }
    public void setUpdateTime(String updateTime) { this.updateTime = updateTime; }
    public String getEndTime() { return endTime; }
    public void setEndTime(String endTime) { this.endTime = endTime; }
    public String getOwnerApp() { return ownerApp; }
    public void setOwnerApp(String ownerApp) { this.ownerApp = ownerApp; }
    public String getCreatedBy() { return createdBy; }
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }
    public String getParentWorkflowId() { return parentWorkflowId; }
    public void setParentWorkflowId(String id) { this.parentWorkflowId = id; }
    public String getParentWorkflowTaskId() { return parentWorkflowTaskId; }
    public void setParentWorkflowTaskId(String id) { this.parentWorkflowTaskId = id; }
    public Set<String> getFailedReferenceTaskNames() { return failedReferenceTaskNames; }
    public void setFailedReferenceTaskNames(Set<String> names) { this.failedReferenceTaskNames = names; }
    public String getExternalInputPayloadStoragePath() { return externalInputPayloadStoragePath; }
    public void setExternalInputPayloadStoragePath(String path) { this.externalInputPayloadStoragePath = path; }
    public String getExternalOutputPayloadStoragePath() { return externalOutputPayloadStoragePath; }
    public void setExternalOutputPayloadStoragePath(String path) { this.externalOutputPayloadStoragePath = path; }
    public long getWorkflowVersion() { return workflowVersion; }
    public void setWorkflowVersion(long version) { this.workflowVersion = version; }
    public String getLastRetriedTime() { return lastRetriedTime; }
    public void setLastRetriedTime(String time) { this.lastRetriedTime = time; }
    public String getUpdatedBy() { return updatedBy; }
    public void setUpdatedBy(String updatedBy) { this.updatedBy = updatedBy; }
    public String getFailedTaskId() { return failedTaskId; }
    public void setFailedTaskId(String failedTaskId) { this.failedTaskId = failedTaskId; }
    public String getEvent() { return event; }
    public void setEvent(String event) { this.event = event; }
    public String getTaskToDomain() { return taskToDomain; }
    public void setTaskToDomain(String taskToDomain) { this.taskToDomain = taskToDomain; }
}
```

---

## 4. Service Interfaces

### 4.1 WorkflowService.java

```java
package io.temporal.conductor.service;

import io.temporal.conductor.dto.*;
import java.util.List;

public interface WorkflowService {
    String startWorkflow(StartWorkflowRequest request);
    Workflow getWorkflow(String workflowId, boolean includeTasks);
    WorkflowStatus getWorkflowStatus(String workflowId);
    void terminateWorkflow(String workflowId, String reason);
    void pauseWorkflow(String workflowId);
    void resumeWorkflow(String workflowId);
    String restartWorkflow(String workflowId, boolean useLatestDefinitions);
    void retryWorkflow(String workflowId, boolean resumeSubworkflowTasks);
    List<String> getRunningWorkflows(String name, Integer version, Long startTime, Long endTime);
    SearchResult<WorkflowSummary> searchWorkflows(int start, int size, String sort, String freeText, String query);
    SearchResult<Workflow> searchWorkflowsV2(int start, int size, String sort, String freeText, String query);
}
```

### 4.2 MetadataService.java

```java
package io.temporal.conductor.service;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import java.util.List;

public interface MetadataService {
    // Workflow definitions
    List<WorkflowDef> getAllWorkflowDefs();
    WorkflowDef getWorkflowDef(String name, int version);
    WorkflowDef getLatestWorkflowDef(String name);
    void registerWorkflowDef(WorkflowDef workflowDef);
    void updateWorkflowDefs(List<WorkflowDef> workflowDefs);
    void deleteWorkflowDef(String name, int version);

    // Task definitions
    List<TaskDef> getAllTaskDefs();
    TaskDef getTaskDef(String taskType);
    void registerTaskDefs(List<TaskDef> taskDefs);
    void deleteTaskDef(String taskType);
}
```

---

## 5. Stub Implementations

### 5.1 StubWorkflowService.java

```java
package io.temporal.conductor.service.stub;

import io.temporal.conductor.dto.*;
import io.temporal.conductor.service.WorkflowService;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Profile("stub")
public class StubWorkflowService implements WorkflowService {

    private final Map<String, Workflow> workflows = new ConcurrentHashMap<>();

    @Override
    public String startWorkflow(StartWorkflowRequest request) {
        String workflowId = UUID.randomUUID().toString();
        Workflow workflow = StubDataGenerator.createWorkflow(
            workflowId,
            request.getName() != null ? request.getName() : "default-workflow",
            "RUNNING"
        );
        workflow.setInput(request.getInput());
        workflow.setCorrelationId(request.getCorrelationId());
        workflow.setPriority(request.getPriority() != null ? request.getPriority() : 0);
        workflows.put(workflowId, workflow);
        return workflowId;
    }

    @Override
    public Workflow getWorkflow(String workflowId, boolean includeTasks) {
        Workflow workflow = workflows.get(workflowId);
        if (workflow == null) {
            workflow = StubDataGenerator.createWorkflow(workflowId, "sample-workflow", "COMPLETED");
        }
        if (!includeTasks) {
            workflow.setTasks(Collections.emptyList());
        }
        return workflow;
    }

    @Override
    public WorkflowStatus getWorkflowStatus(String workflowId) {
        Workflow workflow = getWorkflow(workflowId, false);
        WorkflowStatus status = new WorkflowStatus();
        status.setWorkflowId(workflowId);
        status.setStatus(workflow.getStatus());
        return status;
    }

    @Override
    public void terminateWorkflow(String workflowId, String reason) {
        Workflow workflow = workflows.get(workflowId);
        if (workflow != null) {
            workflow.setStatus("TERMINATED");
            workflow.setReasonForIncompletion(reason);
        }
    }

    @Override
    public void pauseWorkflow(String workflowId) {
        Workflow workflow = workflows.get(workflowId);
        if (workflow != null) {
            workflow.setStatus("PAUSED");
        }
    }

    @Override
    public void resumeWorkflow(String workflowId) {
        Workflow workflow = workflows.get(workflowId);
        if (workflow != null && "PAUSED".equals(workflow.getStatus())) {
            workflow.setStatus("RUNNING");
        }
    }

    @Override
    public String restartWorkflow(String workflowId, boolean useLatestDefinitions) {
        String newId = UUID.randomUUID().toString();
        Workflow oldWorkflow = workflows.get(workflowId);
        if (oldWorkflow != null) {
            Workflow newWorkflow = StubDataGenerator.createWorkflow(
                newId, oldWorkflow.getWorkflowType(), "RUNNING"
            );
            newWorkflow.setInput(oldWorkflow.getInput());
            workflows.put(newId, newWorkflow);
        }
        return newId;
    }

    @Override
    public void retryWorkflow(String workflowId, boolean resumeSubworkflowTasks) {
        Workflow workflow = workflows.get(workflowId);
        if (workflow != null && "FAILED".equals(workflow.getStatus())) {
            workflow.setStatus("RUNNING");
        }
    }

    @Override
    public List<String> getRunningWorkflows(String name, Integer version, Long startTime, Long endTime) {
        return workflows.entrySet().stream()
            .filter(e -> "RUNNING".equals(e.getValue().getStatus()))
            .filter(e -> name == null || name.equals(e.getValue().getWorkflowType()))
            .map(Map.Entry::getKey)
            .toList();
    }

    @Override
    public SearchResult<WorkflowSummary> searchWorkflows(int start, int size, String sort, String freeText, String query) {
        List<WorkflowSummary> results = StubDataGenerator.createWorkflowSummaries(size);
        return new SearchResult<>(results.size() + 50, results);
    }

    @Override
    public SearchResult<Workflow> searchWorkflowsV2(int start, int size, String sort, String freeText, String query) {
        List<Workflow> results = new ArrayList<>();
        for (int i = 0; i < Math.min(size, 10); i++) {
            results.add(StubDataGenerator.createWorkflow(
                UUID.randomUUID().toString(),
                "sample-workflow-" + i,
                i % 3 == 0 ? "RUNNING" : (i % 3 == 1 ? "COMPLETED" : "FAILED")
            ));
        }
        return new SearchResult<>(results.size() + 50, results);
    }
}
```

### 5.2 StubDataGenerator.java

```java
package io.temporal.conductor.service.stub;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import io.temporal.conductor.dto.Workflow;
import io.temporal.conductor.dto.WorkflowSummary;

import java.time.Instant;
import java.util.*;

public final class StubDataGenerator {

    private StubDataGenerator() {}

    public static Workflow createWorkflow(String workflowId, String workflowType, String status) {
        Workflow workflow = new Workflow();
        workflow.setWorkflowId(workflowId);
        workflow.setWorkflowType(workflowType);
        workflow.setVersion(1);
        workflow.setStatus(status);
        workflow.setCreateTime(Instant.now().minusSeconds(3600).toString());
        workflow.setUpdateTime(Instant.now().toString());
        workflow.setInput(Map.of("key1", "value1", "key2", 123));
        workflow.setOutput(Map.of("result", "success"));
        workflow.setCorrelationId("correlation-" + workflowId.substring(0, 8));
        workflow.setPriority(5);
        workflow.setOwnerApp("test-app");
        workflow.setCreatedBy("test-user");

        // Add sample tasks
        workflow.setTasks(createTasks(workflowId, status));

        // Add workflow definition
        workflow.setWorkflowDefinition(createWorkflowDef(workflowType));

        return workflow;
    }

    public static List<Task> createTasks(String workflowId, String workflowStatus) {
        List<Task> tasks = new ArrayList<>();

        Task task1 = new Task();
        task1.setTaskId(workflowId + "-task-1");
        task1.setTaskType("SIMPLE");
        task1.setTaskDefName("simple_task_1");
        task1.setReferenceTaskName("task1_ref");
        task1.setStatus("RUNNING".equals(workflowStatus) ? Task.Status.IN_PROGRESS : Task.Status.COMPLETED);
        task1.setScheduledTime(System.currentTimeMillis() - 60000);
        task1.setStartTime(System.currentTimeMillis() - 55000);
        task1.setEndTime("RUNNING".equals(workflowStatus) ? 0 : System.currentTimeMillis() - 50000);
        task1.setInputData(Map.of("input1", "value1"));
        task1.setOutputData(Map.of("output1", "result1"));
        task1.setWorkflowInstanceId(workflowId);
        tasks.add(task1);

        Task task2 = new Task();
        task2.setTaskId(workflowId + "-task-2");
        task2.setTaskType("SIMPLE");
        task2.setTaskDefName("simple_task_2");
        task2.setReferenceTaskName("task2_ref");
        task2.setStatus("RUNNING".equals(workflowStatus) ? Task.Status.SCHEDULED : Task.Status.COMPLETED);
        task2.setScheduledTime(System.currentTimeMillis() - 30000);
        task2.setWorkflowInstanceId(workflowId);
        tasks.add(task2);

        return tasks;
    }

    public static WorkflowDef createWorkflowDef(String name) {
        WorkflowDef def = new WorkflowDef();
        def.setName(name);
        def.setVersion(1);
        def.setDescription("Sample workflow definition for " + name);
        def.setOwnerEmail("owner@example.com");
        def.setTimeoutSeconds(3600);

        List<WorkflowTask> tasks = new ArrayList<>();

        WorkflowTask task1 = new WorkflowTask();
        task1.setName("simple_task_1");
        task1.setTaskReferenceName("task1_ref");
        task1.setType("SIMPLE");
        tasks.add(task1);

        WorkflowTask task2 = new WorkflowTask();
        task2.setName("simple_task_2");
        task2.setTaskReferenceName("task2_ref");
        task2.setType("SIMPLE");
        tasks.add(task2);

        def.setTasks(tasks);
        return def;
    }

    public static List<WorkflowSummary> createWorkflowSummaries(int count) {
        List<WorkflowSummary> summaries = new ArrayList<>();
        String[] statuses = {"RUNNING", "COMPLETED", "FAILED", "PAUSED"};

        for (int i = 0; i < count; i++) {
            WorkflowSummary summary = new WorkflowSummary();
            summary.setWorkflowId(UUID.randomUUID().toString());
            summary.setWorkflowType("sample-workflow-" + (i % 5));
            summary.setVersion(1);
            summary.setStatus(statuses[i % statuses.length]);
            summary.setStartTime(Instant.now().minusSeconds(3600 - i * 60).toString());
            summary.setUpdateTime(Instant.now().minusSeconds(i * 30).toString());
            summary.setCorrelationId("correlation-" + i);
            summary.setPriority(i % 10);
            summaries.add(summary);
        }

        return summaries;
    }

    public static List<WorkflowDef> createWorkflowDefs() {
        List<WorkflowDef> defs = new ArrayList<>();
        defs.add(createWorkflowDef("order-processing"));
        defs.add(createWorkflowDef("payment-workflow"));
        defs.add(createWorkflowDef("notification-workflow"));
        defs.add(createWorkflowDef("data-pipeline"));
        return defs;
    }

    public static List<TaskDef> createTaskDefs() {
        List<TaskDef> defs = new ArrayList<>();

        TaskDef task1 = new TaskDef();
        task1.setName("simple_task_1");
        task1.setDescription("Simple task 1");
        task1.setRetryCount(3);
        task1.setTimeoutSeconds(60);
        defs.add(task1);

        TaskDef task2 = new TaskDef();
        task2.setName("simple_task_2");
        task2.setDescription("Simple task 2");
        task2.setRetryCount(3);
        task2.setTimeoutSeconds(120);
        defs.add(task2);

        TaskDef httpTask = new TaskDef();
        httpTask.setName("http_task");
        httpTask.setDescription("HTTP task");
        httpTask.setRetryCount(2);
        httpTask.setTimeoutSeconds(30);
        defs.add(httpTask);

        return defs;
    }
}
```

---

## 6. Configuration

### 6.1 application.yml

```yaml
server:
  port: 8080

spring:
  profiles:
    active: stub  # Use stub by default

logging:
  level:
    io.temporal.conductor: DEBUG
    org.springframework.web: INFO

# Temporal configuration (for non-stub mode)
temporal:
  namespace: conductor
  service-address: localhost:7233
  task-queue: conductor-workflows

# Swagger/OpenAPI
springdoc:
  api-docs:
    path: /api-docs
  swagger-ui:
    path: /swagger-ui.html
```

### 6.2 ServiceConfig.java

```java
package io.temporal.conductor.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
public class ServiceConfig {
    // Beans configured via @Profile annotations in service classes
}
```

---

## 7. Test Cases

### 7.1 WorkflowResourceTest.java

```java
package io.temporal.conductor.api;

import io.temporal.conductor.dto.SearchResult;
import io.temporal.conductor.dto.Workflow;
import io.temporal.conductor.dto.WorkflowSummary;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("stub")
class WorkflowResourceTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void startWorkflow() throws Exception {
        mockMvc.perform(post("/api/workflow")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\": \"test-workflow\", \"input\": {\"key\": \"value\"}}"))
            .andExpect(status().isOk())
            .andExpect(content().string(org.hamcrest.Matchers.notNullValue()));
    }

    @Test
    void getWorkflow() throws Exception {
        // First start a workflow
        String workflowId = mockMvc.perform(post("/api/workflow")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\": \"test-workflow\"}"))
            .andReturn().getResponse().getContentAsString();

        // Then get it
        mockMvc.perform(get("/api/workflow/" + workflowId))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.workflowId").value(workflowId))
            .andExpect(jsonPath("$.status").exists());
    }

    @Test
    void searchWorkflows() throws Exception {
        mockMvc.perform(get("/api/workflow/search")
                .param("start", "0")
                .param("size", "10"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.results").isArray())
            .andExpect(jsonPath("$.totalHits").isNumber());
    }

    @Test
    void pauseAndResumeWorkflow() throws Exception {
        String workflowId = mockMvc.perform(post("/api/workflow")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\": \"test-workflow\"}"))
            .andReturn().getResponse().getContentAsString();

        mockMvc.perform(put("/api/workflow/" + workflowId + "/pause"))
            .andExpect(status().isOk());

        mockMvc.perform(put("/api/workflow/" + workflowId + "/resume"))
            .andExpect(status().isOk());
    }

    @Test
    void terminateWorkflow() throws Exception {
        String workflowId = mockMvc.perform(post("/api/workflow")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\": \"test-workflow\"}"))
            .andReturn().getResponse().getContentAsString();

        mockMvc.perform(delete("/api/workflow/" + workflowId)
                .param("reason", "Test termination"))
            .andExpect(status().isNoContent());
    }
}
```

---

## 8. Acceptance Criteria

- [ ] All REST endpoints return expected responses
- [ ] Stub services generate realistic mock data
- [ ] Spring Boot application starts successfully
- [ ] Swagger UI accessible at `/swagger-ui.html`
- [ ] All API tests pass
- [ ] Checkstyle passes

---

## 9. Implementation Checklist

1. [ ] Update `build.gradle` with Spring Boot dependencies
2. [ ] Create `application.yml` configuration
3. [ ] Create DTO classes (StartWorkflowRequest, SearchResult, WorkflowSummary, Workflow, etc.)
4. [ ] Create service interfaces (WorkflowService, MetadataService, TaskService)
5. [ ] Create stub implementations (StubWorkflowService, StubMetadataService)
6. [ ] Create StubDataGenerator
7. [ ] Create REST controllers (WorkflowResource, MetadataResource, TaskResource)
8. [ ] Create Spring Boot Application class
9. [ ] Create API tests
10. [ ] Verify Swagger UI works
11. [ ] Run all tests
