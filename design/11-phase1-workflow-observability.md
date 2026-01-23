# Phase 1: Workflow Observability

## Overview

This phase extends `ConductorWorkflow` to support UI integration by adding:
1. **Search Attributes** - Enable visibility queries for workflow listing/filtering
2. **Query Methods** - Return full workflow state for UI display
3. **Signal Methods** - Control workflow execution (pause/resume/retry)

**Language**: Java (consistent with Conductor codebase)

## Project Structure

Create new module: `conductor-server-temporal`

```
conductor-server-temporal/
├── build.gradle
├── src/main/java/io/temporal/conductor/
│   ├── workflow/
│   │   ├── ConductorWorkflow.java          # Workflow interface
│   │   ├── ConductorWorkflowImpl.java      # Workflow implementation
│   │   ├── ConductorSearchAttributes.java  # Search attribute keys
│   │   └── model/
│   │       ├── WorkflowState.java          # Full workflow state DTO
│   │       ├── TaskState.java              # Task state DTO
│   │       ├── ConductorWorkflowInput.java # Workflow input
│   │       └── ConductorWorkflowOutput.java# Workflow output
│   ├── activity/
│   │   ├── TaskExecutionActivities.java    # Activity interface
│   │   └── TaskExecutionActivitiesImpl.java# Activity implementation
│   └── executor/
│       ├── SystemTaskExecutor.java         # System task delegation
│       ├── InMemoryMetadataDAO.java        # Metadata storage
│       ├── InMemoryExecutionDAOFacade.java # Execution facade
│       └── InMemoryWorkflowExecutor.java   # Workflow executor
└── src/test/java/io/temporal/conductor/
    └── workflow/
        ├── WorkflowObservabilityTest.java  # Phase 1 tests
        ├── SearchAttributeTest.java        # Search attribute tests
        └── SignalTest.java                 # Signal/query tests
```

---

## 1. Search Attributes

### 1.1 Required Attributes

| Attribute | Type | Source | Purpose |
|-----------|------|--------|---------|
| `ConductorWorkflowType` | Keyword | `workflowDef.name` | Filter by workflow type |
| `ConductorWorkflowVersion` | Int | `workflowDef.version` | Filter by version |
| `ConductorStatus` | Keyword | `workflowModel.status` | Filter by status |
| `ConductorCorrelationId` | Keyword | `input.correlationId` | Group related workflows |
| `ConductorPriority` | Int | `input.priority` | Sort by priority |
| `ConductorFailedTaskNames` | KeywordList | Failed task refs | Filter by failure type |
| `ConductorOwnerApp` | Keyword | `input.ownerApp` | Multi-tenant filtering |

### 1.2 Registration Script

```bash
# Register search attributes in Temporal
temporal operator search-attribute create \
  --namespace conductor \
  --name ConductorWorkflowType --type Keyword \
  --name ConductorWorkflowVersion --type Int \
  --name ConductorStatus --type Keyword \
  --name ConductorCorrelationId --type Keyword \
  --name ConductorPriority --type Int \
  --name ConductorFailedTaskNames --type KeywordList \
  --name ConductorOwnerApp --type Keyword
```

### 1.3 Java Definitions

```java
// ConductorSearchAttributes.java
package io.temporal.conductor.workflow;

import io.temporal.common.SearchAttributeKey;
import java.util.List;

public final class ConductorSearchAttributes {

    public static final SearchAttributeKey<String> WORKFLOW_TYPE =
        SearchAttributeKey.forKeyword("ConductorWorkflowType");

    public static final SearchAttributeKey<Long> WORKFLOW_VERSION =
        SearchAttributeKey.forLong("ConductorWorkflowVersion");

    public static final SearchAttributeKey<String> STATUS =
        SearchAttributeKey.forKeyword("ConductorStatus");

    public static final SearchAttributeKey<String> CORRELATION_ID =
        SearchAttributeKey.forKeyword("ConductorCorrelationId");

    public static final SearchAttributeKey<Long> PRIORITY =
        SearchAttributeKey.forLong("ConductorPriority");

    public static final SearchAttributeKey<List<String>> FAILED_TASK_NAMES =
        SearchAttributeKey.forKeywordList("ConductorFailedTaskNames");

    public static final SearchAttributeKey<String> OWNER_APP =
        SearchAttributeKey.forKeyword("ConductorOwnerApp");

    private ConductorSearchAttributes() {} // Prevent instantiation
}
```

---

## 2. Workflow Interface

### 2.1 ConductorWorkflow.java

```java
package io.temporal.conductor.workflow;

import io.temporal.conductor.workflow.model.ConductorWorkflowInput;
import io.temporal.conductor.workflow.model.ConductorWorkflowOutput;
import io.temporal.conductor.workflow.model.TaskState;
import io.temporal.conductor.workflow.model.WorkflowState;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

import java.util.List;
import java.util.Map;

@WorkflowInterface
public interface ConductorWorkflow {

    @WorkflowMethod
    ConductorWorkflowOutput execute(ConductorWorkflowInput input);

    // === Signal Methods ===

    @SignalMethod
    void completeTask(String taskRefName, Map<String, Object> output);

    @SignalMethod
    void pause();

    @SignalMethod
    void resume();

    @SignalMethod
    void retryFailedTask(String taskRefName);

    // === Query Methods ===

    @QueryMethod
    WorkflowState getWorkflow();

    @QueryMethod
    List<TaskState> getTasks();

    @QueryMethod
    Map<String, Object> getVariables();
}
```

---

## 3. Data Models

### 3.1 WorkflowState.java

```java
package io.temporal.conductor.workflow.model;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Full workflow state for UI display.
 * Maps to Conductor's Workflow object structure.
 */
public class WorkflowState {

    // Identity
    private String workflowId;
    private String workflowType;
    private int version;

    // Status
    private String status;
    private String reasonForIncompletion;

    // Timing
    private long createTime;
    private long startTime;
    private long updateTime;
    private Long endTime;

    // Data
    private Map<String, Object> input;
    private Map<String, Object> output;
    private Map<String, Object> variables;

    // Metadata
    private String correlationId;
    private int priority;
    private String ownerApp;
    private String createdBy;

    // Tasks
    private List<TaskState> tasks;

    // Default constructor for Jackson
    public WorkflowState() {}

    // Builder pattern
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final WorkflowState state = new WorkflowState();

        public Builder workflowId(String workflowId) {
            state.workflowId = workflowId;
            return this;
        }

        public Builder workflowType(String workflowType) {
            state.workflowType = workflowType;
            return this;
        }

        public Builder version(int version) {
            state.version = version;
            return this;
        }

        public Builder status(String status) {
            state.status = status;
            return this;
        }

        public Builder reasonForIncompletion(String reason) {
            state.reasonForIncompletion = reason;
            return this;
        }

        public Builder createTime(long createTime) {
            state.createTime = createTime;
            return this;
        }

        public Builder startTime(long startTime) {
            state.startTime = startTime;
            return this;
        }

        public Builder updateTime(long updateTime) {
            state.updateTime = updateTime;
            return this;
        }

        public Builder endTime(Long endTime) {
            state.endTime = endTime;
            return this;
        }

        public Builder input(Map<String, Object> input) {
            state.input = input;
            return this;
        }

        public Builder output(Map<String, Object> output) {
            state.output = output;
            return this;
        }

        public Builder variables(Map<String, Object> variables) {
            state.variables = variables;
            return this;
        }

        public Builder correlationId(String correlationId) {
            state.correlationId = correlationId;
            return this;
        }

        public Builder priority(int priority) {
            state.priority = priority;
            return this;
        }

        public Builder ownerApp(String ownerApp) {
            state.ownerApp = ownerApp;
            return this;
        }

        public Builder createdBy(String createdBy) {
            state.createdBy = createdBy;
            return this;
        }

        public Builder tasks(List<TaskState> tasks) {
            state.tasks = tasks;
            return this;
        }

        public WorkflowState build() {
            return state;
        }
    }

    // Getters
    public String getWorkflowId() { return workflowId; }
    public String getWorkflowType() { return workflowType; }
    public int getVersion() { return version; }
    public String getStatus() { return status; }
    public String getReasonForIncompletion() { return reasonForIncompletion; }
    public long getCreateTime() { return createTime; }
    public long getStartTime() { return startTime; }
    public long getUpdateTime() { return updateTime; }
    public Long getEndTime() { return endTime; }
    public Map<String, Object> getInput() { return input; }
    public Map<String, Object> getOutput() { return output; }
    public Map<String, Object> getVariables() { return variables; }
    public String getCorrelationId() { return correlationId; }
    public int getPriority() { return priority; }
    public String getOwnerApp() { return ownerApp; }
    public String getCreatedBy() { return createdBy; }
    public List<TaskState> getTasks() { return tasks; }

    // Setters for Jackson
    public void setWorkflowId(String workflowId) { this.workflowId = workflowId; }
    public void setWorkflowType(String workflowType) { this.workflowType = workflowType; }
    public void setVersion(int version) { this.version = version; }
    public void setStatus(String status) { this.status = status; }
    public void setReasonForIncompletion(String reason) { this.reasonForIncompletion = reason; }
    public void setCreateTime(long createTime) { this.createTime = createTime; }
    public void setStartTime(long startTime) { this.startTime = startTime; }
    public void setUpdateTime(long updateTime) { this.updateTime = updateTime; }
    public void setEndTime(Long endTime) { this.endTime = endTime; }
    public void setInput(Map<String, Object> input) { this.input = input; }
    public void setOutput(Map<String, Object> output) { this.output = output; }
    public void setVariables(Map<String, Object> variables) { this.variables = variables; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    public void setPriority(int priority) { this.priority = priority; }
    public void setOwnerApp(String ownerApp) { this.ownerApp = ownerApp; }
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }
    public void setTasks(List<TaskState> tasks) { this.tasks = tasks; }
}
```

### 3.2 TaskState.java

```java
package io.temporal.conductor.workflow.model;

import java.util.List;
import java.util.Map;

/**
 * Task state for UI display.
 * Maps to Conductor's Task object structure.
 */
public class TaskState {

    // Identity
    private String taskId;
    private String taskType;
    private String taskDefName;
    private String referenceTaskName;

    // Status
    private String status;
    private String reasonForIncompletion;
    private int retryCount;

    // Timing
    private long scheduledTime;
    private long startTime;
    private long updateTime;
    private long endTime;

    // Data
    private Map<String, Object> inputData;
    private Map<String, Object> outputData;

    // Execution info
    private String workerId;
    private int pollCount;
    private int iteration;

    // Logs
    private List<TaskLog> logs;

    // Default constructor
    public TaskState() {}

    // Builder pattern
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final TaskState state = new TaskState();

        public Builder taskId(String taskId) {
            state.taskId = taskId;
            return this;
        }

        public Builder taskType(String taskType) {
            state.taskType = taskType;
            return this;
        }

        public Builder taskDefName(String taskDefName) {
            state.taskDefName = taskDefName;
            return this;
        }

        public Builder referenceTaskName(String referenceTaskName) {
            state.referenceTaskName = referenceTaskName;
            return this;
        }

        public Builder status(String status) {
            state.status = status;
            return this;
        }

        public Builder reasonForIncompletion(String reason) {
            state.reasonForIncompletion = reason;
            return this;
        }

        public Builder retryCount(int retryCount) {
            state.retryCount = retryCount;
            return this;
        }

        public Builder scheduledTime(long scheduledTime) {
            state.scheduledTime = scheduledTime;
            return this;
        }

        public Builder startTime(long startTime) {
            state.startTime = startTime;
            return this;
        }

        public Builder updateTime(long updateTime) {
            state.updateTime = updateTime;
            return this;
        }

        public Builder endTime(long endTime) {
            state.endTime = endTime;
            return this;
        }

        public Builder inputData(Map<String, Object> inputData) {
            state.inputData = inputData;
            return this;
        }

        public Builder outputData(Map<String, Object> outputData) {
            state.outputData = outputData;
            return this;
        }

        public Builder workerId(String workerId) {
            state.workerId = workerId;
            return this;
        }

        public Builder pollCount(int pollCount) {
            state.pollCount = pollCount;
            return this;
        }

        public Builder iteration(int iteration) {
            state.iteration = iteration;
            return this;
        }

        public Builder logs(List<TaskLog> logs) {
            state.logs = logs;
            return this;
        }

        public TaskState build() {
            return state;
        }
    }

    // Getters and setters omitted for brevity - follow same pattern as WorkflowState
    public String getTaskId() { return taskId; }
    public String getTaskType() { return taskType; }
    public String getTaskDefName() { return taskDefName; }
    public String getReferenceTaskName() { return referenceTaskName; }
    public String getStatus() { return status; }
    public String getReasonForIncompletion() { return reasonForIncompletion; }
    public int getRetryCount() { return retryCount; }
    public long getScheduledTime() { return scheduledTime; }
    public long getStartTime() { return startTime; }
    public long getUpdateTime() { return updateTime; }
    public long getEndTime() { return endTime; }
    public Map<String, Object> getInputData() { return inputData; }
    public Map<String, Object> getOutputData() { return outputData; }
    public String getWorkerId() { return workerId; }
    public int getPollCount() { return pollCount; }
    public int getIteration() { return iteration; }
    public List<TaskLog> getLogs() { return logs; }

    public void setTaskId(String taskId) { this.taskId = taskId; }
    public void setTaskType(String taskType) { this.taskType = taskType; }
    public void setTaskDefName(String taskDefName) { this.taskDefName = taskDefName; }
    public void setReferenceTaskName(String referenceTaskName) { this.referenceTaskName = referenceTaskName; }
    public void setStatus(String status) { this.status = status; }
    public void setReasonForIncompletion(String reason) { this.reasonForIncompletion = reason; }
    public void setRetryCount(int retryCount) { this.retryCount = retryCount; }
    public void setScheduledTime(long scheduledTime) { this.scheduledTime = scheduledTime; }
    public void setStartTime(long startTime) { this.startTime = startTime; }
    public void setUpdateTime(long updateTime) { this.updateTime = updateTime; }
    public void setEndTime(long endTime) { this.endTime = endTime; }
    public void setInputData(Map<String, Object> inputData) { this.inputData = inputData; }
    public void setOutputData(Map<String, Object> outputData) { this.outputData = outputData; }
    public void setWorkerId(String workerId) { this.workerId = workerId; }
    public void setPollCount(int pollCount) { this.pollCount = pollCount; }
    public void setIteration(int iteration) { this.iteration = iteration; }
    public void setLogs(List<TaskLog> logs) { this.logs = logs; }
}

/**
 * Task execution log entry.
 */
class TaskLog {
    private String log;
    private long createdTime;

    public TaskLog() {}

    public TaskLog(String log, long createdTime) {
        this.log = log;
        this.createdTime = createdTime;
    }

    public String getLog() { return log; }
    public void setLog(String log) { this.log = log; }
    public long getCreatedTime() { return createdTime; }
    public void setCreatedTime(long createdTime) { this.createdTime = createdTime; }
}
```

### 3.3 ConductorWorkflowInput.java

```java
package io.temporal.conductor.workflow.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConductorWorkflowInput {

    private String workflowDefJson = "";
    private Map<String, Object> workflowInput = Collections.emptyMap();
    private Map<String, String> taskDefsJson = Collections.emptyMap();

    // Metadata for search attributes
    private String correlationId;
    private Integer priority;
    private String ownerApp;
    private String createdBy;
    private List<String> tags;

    public ConductorWorkflowInput() {}

    // Getters
    public String getWorkflowDefJson() { return workflowDefJson; }
    public Map<String, Object> getWorkflowInput() { return workflowInput; }
    public Map<String, String> getTaskDefsJson() { return taskDefsJson; }
    public String getCorrelationId() { return correlationId; }
    public Integer getPriority() { return priority; }
    public String getOwnerApp() { return ownerApp; }
    public String getCreatedBy() { return createdBy; }
    public List<String> getTags() { return tags; }

    // Setters
    public void setWorkflowDefJson(String workflowDefJson) { this.workflowDefJson = workflowDefJson; }
    public void setWorkflowInput(Map<String, Object> workflowInput) { this.workflowInput = workflowInput; }
    public void setTaskDefsJson(Map<String, String> taskDefsJson) { this.taskDefsJson = taskDefsJson; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    public void setPriority(Integer priority) { this.priority = priority; }
    public void setOwnerApp(String ownerApp) { this.ownerApp = ownerApp; }
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }
    public void setTags(List<String> tags) { this.tags = tags; }

    // Builder
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final ConductorWorkflowInput input = new ConductorWorkflowInput();

        public Builder workflowDefJson(String json) {
            input.workflowDefJson = json;
            return this;
        }

        public Builder workflowInput(Map<String, Object> workflowInput) {
            input.workflowInput = workflowInput;
            return this;
        }

        public Builder taskDefsJson(Map<String, String> taskDefsJson) {
            input.taskDefsJson = taskDefsJson;
            return this;
        }

        public Builder correlationId(String correlationId) {
            input.correlationId = correlationId;
            return this;
        }

        public Builder priority(Integer priority) {
            input.priority = priority;
            return this;
        }

        public Builder ownerApp(String ownerApp) {
            input.ownerApp = ownerApp;
            return this;
        }

        public Builder createdBy(String createdBy) {
            input.createdBy = createdBy;
            return this;
        }

        public Builder tags(List<String> tags) {
            input.tags = tags;
            return this;
        }

        public ConductorWorkflowInput build() {
            return input;
        }
    }
}
```

---

## 4. Implementation Requirements

### 4.1 Search Attribute Initialization

In `ConductorWorkflowImpl.execute()`, after parsing workflow definition:

```java
private void initializeSearchAttributes(WorkflowDef workflowDef, ConductorWorkflowInput input) {
    Workflow.upsertTypedSearchAttributes(
        ConductorSearchAttributes.WORKFLOW_TYPE.valueSet(workflowDef.getName()),
        ConductorSearchAttributes.WORKFLOW_VERSION.valueSet((long) workflowDef.getVersion()),
        ConductorSearchAttributes.STATUS.valueSet("RUNNING"),
        ConductorSearchAttributes.CORRELATION_ID.valueSet(
            input.getCorrelationId() != null ? input.getCorrelationId() : ""),
        ConductorSearchAttributes.PRIORITY.valueSet(
            input.getPriority() != null ? input.getPriority().longValue() : 0L),
        ConductorSearchAttributes.OWNER_APP.valueSet(
            input.getOwnerApp() != null ? input.getOwnerApp() : "")
    );
}
```

### 4.2 Status Update

```java
private void updateStatusAttribute(WorkflowModel.Status status) {
    Workflow.upsertTypedSearchAttributes(
        ConductorSearchAttributes.STATUS.valueSet(status.name())
    );
}
```

### 4.3 Pause Handling in Scheduling Loop

```java
private void runSchedulingLoop() {
    while (!workflowModel.getStatus().isTerminal() && iterations < MAX_ITERATIONS) {
        // Check for pause
        if (isPaused) {
            Workflow.await(() -> !isPaused);
            continue;
        }

        // ... rest of scheduling loop
    }
}
```

### 4.4 Query Method Implementations

```java
@Override
public WorkflowState getWorkflow() {
    return WorkflowState.builder()
        .workflowId(workflowModel.getWorkflowId())
        .workflowType(workflowModel.getWorkflowName())
        .version(workflowModel.getWorkflowVersion())
        .status(workflowModel.getStatus().name())
        .reasonForIncompletion(workflowModel.getReasonForIncompletion())
        .createTime(workflowModel.getCreateTime())
        .startTime(workflowModel.getStartTime())
        .updateTime(workflowModel.getUpdateTime())
        .endTime(workflowModel.getStatus().isTerminal() ? workflowModel.getEndTime() : null)
        .input(workflowModel.getInput())
        .output(workflowModel.getOutput())
        .variables(workflowModel.getVariables())
        .correlationId(workflowModel.getCorrelationId())
        .priority(workflowModel.getPriority())
        .ownerApp(workflowModel.getOwnerApp())
        .createdBy(workflowModel.getCreatedBy())
        .tasks(workflowModel.getTasks().stream()
            .map(this::toTaskState)
            .collect(Collectors.toList()))
        .build();
}

@Override
public List<TaskState> getTasks() {
    return workflowModel.getTasks().stream()
        .map(this::toTaskState)
        .collect(Collectors.toList());
}

@Override
public Map<String, Object> getVariables() {
    return workflowModel.getVariables() != null
        ? workflowModel.getVariables()
        : Collections.emptyMap();
}

private TaskState toTaskState(TaskModel task) {
    return TaskState.builder()
        .taskId(task.getTaskId())
        .taskType(task.getTaskType())
        .taskDefName(task.getTaskDefName())
        .referenceTaskName(task.getReferenceTaskName())
        .status(task.getStatus().name())
        .reasonForIncompletion(task.getReasonForIncompletion())
        .retryCount(task.getRetryCount())
        .scheduledTime(task.getScheduledTime())
        .startTime(task.getStartTime())
        .updateTime(task.getUpdateTime())
        .endTime(task.getEndTime())
        .inputData(task.getInputData())
        .outputData(task.getOutputData())
        .workerId(task.getWorkerId())
        .pollCount(task.getPollCount())
        .iteration(task.getIteration())
        .build();
}
```

### 4.5 Signal Method Implementations

```java
private volatile boolean isPaused = false;

@Override
public void pause() {
    if (workflowModel.getStatus() == WorkflowModel.Status.RUNNING) {
        isPaused = true;
        workflowModel.setStatus(WorkflowModel.Status.PAUSED);
        updateStatusAttribute(WorkflowModel.Status.PAUSED);
        logger.info("Workflow paused: {}", workflowModel.getWorkflowId());
    }
}

@Override
public void resume() {
    if (isPaused) {
        isPaused = false;
        workflowModel.setStatus(WorkflowModel.Status.RUNNING);
        updateStatusAttribute(WorkflowModel.Status.RUNNING);
        logger.info("Workflow resumed: {}", workflowModel.getWorkflowId());
    }
}

@Override
public void retryFailedTask(String taskRefName) {
    Optional<TaskModel> failedTask = workflowModel.getTasks().stream()
        .filter(t -> t.getReferenceTaskName().equals(taskRefName))
        .filter(t -> t.getStatus() == TaskModel.Status.FAILED ||
                     t.getStatus() == TaskModel.Status.FAILED_WITH_TERMINAL_ERROR)
        .findFirst();

    failedTask.ifPresent(task -> {
        task.setStatus(TaskModel.Status.SCHEDULED);
        task.setRetryCount(task.getRetryCount() + 1);
        task.setReasonForIncompletion(null);
        task.setStartTime(0);
        task.setEndTime(0);
        logger.info("Task {} scheduled for retry (attempt {})",
            taskRefName, task.getRetryCount());
    });
}
```

---

## 5. Build Configuration

### 5.1 build.gradle

```groovy
plugins {
    id 'java'
    id 'checkstyle'
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

repositories {
    mavenCentral()
}

dependencies {
    // Temporal SDK
    implementation 'io.temporal:temporal-sdk:1.25.0'

    // Conductor dependencies
    implementation 'org.conductoross:conductor-core:3.21.23'
    implementation 'org.conductoross:conductor-common:3.21.23'

    // Jackson for JSON
    implementation 'com.fasterxml.jackson.core:jackson-databind:2.17.0'

    // Logging
    implementation 'org.slf4j:slf4j-api:2.0.9'
    runtimeOnly 'ch.qos.logback:logback-classic:1.4.14'

    // Testing
    testImplementation 'io.temporal:temporal-testing:1.25.0'
    testImplementation 'org.junit.jupiter:junit-jupiter:5.10.2'
    testImplementation 'org.mockito:mockito-core:5.8.0'
    testRuntimeOnly 'org.junit.platform:junit-platform-launcher'
}

test {
    useJUnitPlatform()
}

checkstyle {
    toolVersion = '10.12.5'
    configFile = file("${rootDir}/config/checkstyle/checkstyle.xml")
}
```

---

## 6. Test Cases

### 6.1 Search Attribute Tests

```java
@Test
void searchAttributesSetOnWorkflowStart() {
    WorkflowOptions options = WorkflowOptions.newBuilder()
        .setWorkflowId("test-" + UUID.randomUUID())
        .setTaskQueue(TASK_QUEUE)
        .build();

    ConductorWorkflow workflow = client.newWorkflowStub(ConductorWorkflow.class, options);
    WorkflowClient.start(workflow::execute, createSimpleInput());

    WorkflowExecutionInfo info = client.getWorkflowServiceStubs()
        .blockingStub()
        .describeWorkflowExecution(/* ... */)
        .getWorkflowExecutionInfo();

    Map<String, ?> attrs = info.getSearchAttributes();
    assertEquals("simple-workflow", attrs.get("ConductorWorkflowType"));
    assertEquals("RUNNING", attrs.get("ConductorStatus"));
}

@Test
void statusAttributeUpdatesOnCompletion() {
    ConductorWorkflow workflow = client.newWorkflowStub(ConductorWorkflow.class, options);
    workflow.execute(createSimpleInput());  // Blocks until complete

    // Query final status
    assertEquals("COMPLETED", getSearchAttribute(options.getWorkflowId(), "ConductorStatus"));
}
```

### 6.2 Query Method Tests

```java
@Test
void getWorkflowReturnsFullState() {
    ConductorWorkflow workflow = client.newWorkflowStub(ConductorWorkflow.class, options);
    WorkflowClient.start(workflow::execute, createMultiTaskInput());

    Thread.sleep(1000);  // Let tasks execute

    WorkflowState state = workflow.getWorkflow();

    assertEquals("multi-task-workflow", state.getWorkflowType());
    assertEquals("RUNNING", state.getStatus());
    assertFalse(state.getTasks().isEmpty());
    assertTrue(state.getCreateTime() > 0);
}

@Test
void getTasksReturnsAllTasks() {
    ConductorWorkflow workflow = client.newWorkflowStub(ConductorWorkflow.class, options);
    workflow.execute(createSequentialInput());  // Complete workflow

    List<TaskState> tasks = workflow.getTasks();

    assertEquals(2, tasks.size());
    assertTrue(tasks.stream().allMatch(t -> "COMPLETED".equals(t.getStatus())));
}
```

### 6.3 Signal Method Tests

```java
@Test
void pauseAndResumeWorkflow() throws InterruptedException {
    ConductorWorkflow workflow = client.newWorkflowStub(ConductorWorkflow.class, options);
    WorkflowClient.start(workflow::execute, createLongRunningInput());

    Thread.sleep(500);

    // Pause
    workflow.pause();
    Thread.sleep(100);
    assertEquals("PAUSED", workflow.getWorkflow().getStatus());

    // Resume
    workflow.resume();
    Thread.sleep(100);
    assertEquals("RUNNING", workflow.getWorkflow().getStatus());
}

@Test
void retryFailedTask() throws InterruptedException {
    ConductorWorkflow workflow = client.newWorkflowStub(ConductorWorkflow.class, options);
    WorkflowClient.start(workflow::execute, createFailOnceInput());

    Thread.sleep(1000);  // Wait for failure

    List<TaskState> tasks = workflow.getTasks();
    TaskState failedTask = tasks.stream()
        .filter(t -> "FAILED".equals(t.getStatus()))
        .findFirst()
        .orElseThrow();

    // Retry
    workflow.retryFailedTask(failedTask.getReferenceTaskName());

    // Wait for completion
    ConductorWorkflowOutput output = client
        .newUntypedWorkflowStub(options.getWorkflowId())
        .getResult(ConductorWorkflowOutput.class);

    assertEquals("COMPLETED", output.getStatus());
}
```

---

## 7. Acceptance Criteria

- [ ] All Java files compile without errors
- [ ] Checkstyle passes with no violations
- [ ] Search attributes set correctly on workflow start
- [ ] Status attribute updates on workflow state changes
- [ ] `getWorkflow()` returns complete workflow state
- [ ] `getTasks()` returns all task details with timing info
- [ ] `getVariables()` returns current workflow variables
- [ ] `pause()` signal stops workflow execution
- [ ] `resume()` signal continues paused workflow
- [ ] `retryFailedTask()` reschedules failed tasks
- [ ] All unit tests pass

---

## 8. Dependencies

- JDK 21
- Temporal SDK 1.25+
- Conductor Core 3.21.23
- Gradle 8.x

---

## 9. Implementation Checklist

1. [ ] Create `conductor-server-temporal` module with build.gradle
2. [ ] Create `ConductorSearchAttributes.java`
3. [ ] Create `WorkflowState.java`
4. [ ] Create `TaskState.java`
5. [ ] Create `ConductorWorkflowInput.java`
6. [ ] Create `ConductorWorkflowOutput.java`
7. [ ] Create `ConductorWorkflow.java` interface
8. [ ] Create `ConductorWorkflowImpl.java` with:
   - Search attribute initialization
   - Status attribute updates
   - Query method implementations
   - Signal method implementations
   - Pause handling in scheduling loop
9. [ ] Create `TaskExecutionActivities.java` interface
10. [ ] Create `TaskExecutionActivitiesImpl.java`
11. [ ] Port `SystemTaskExecutor` from Kotlin to Java
12. [ ] Port `InMemoryMetadataDAO` from Kotlin to Java
13. [ ] Port `InMemoryExecutionDAOFacade` from Kotlin to Java
14. [ ] Port `InMemoryWorkflowExecutor` from Kotlin to Java
15. [ ] Create test cases
16. [ ] Configure checkstyle
17. [ ] Run tests and verify all pass
