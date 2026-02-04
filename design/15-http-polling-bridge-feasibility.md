# HTTP Polling Bridge for Conductor Workers

## Overview

This document describes how to support existing Conductor workers that use HTTP polling through Temporal's activity gRPC interface, without requiring any changes to the workers themselves.

## Protocol Comparison

### Conductor HTTP Polling

```
┌─────────────────┐         ┌─────────────────┐
│  Conductor      │  HTTP   │  Conductor      │
│  Worker         │◄───────►│  Server         │
└─────────────────┘         └─────────────────┘

1. Worker polls:     GET  /tasks/poll/{taskType}?workerid={id}
2. Server returns:   Task JSON (or empty if no work)
3. Worker executes:  (locally)
4. Worker reports:   POST /tasks  (TaskResult JSON)
```

### Temporal Activity Polling

```
┌─────────────────┐  gRPC   ┌─────────────────┐
│  Temporal       │◄───────►│  Temporal       │
│  Worker         │         │  Server         │
└─────────────────┘         └─────────────────┘

1. Worker calls:     PollActivityTaskQueue (long-poll, up to 60s)
2. Server returns:   PollActivityTaskQueueResponse (or empty)
3. Worker executes:  Activity implementation
4. Worker reports:   RespondActivityTaskCompleted/Failed
```

**Key insight:** Both protocols are poll-based. The main difference is transport (HTTP vs gRPC) and message format.

## Architecture

The bridge wraps Temporal's `PollActivityTaskQueue` gRPC with HTTP endpoints, providing direct protocol translation.

```
┌─────────────────────────────────────────────────────────────────┐
│                  Temporal Conductor Server                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              HTTP Polling Endpoints                       │   │
│  │                                                           │   │
│  │  GET /tasks/poll/{taskType}                               │   │
│  │    └─► Calls PollActivityTaskQueue gRPC (long-poll)       │   │
│  │    └─► Translates response to Conductor Task format       │   │
│  │    └─► Returns taskToken as opaque identifier             │   │
│  │                                                           │   │
│  │  POST /tasks                                              │   │
│  │    └─► Extracts taskToken from TaskResult                 │   │
│  │    └─► Calls RespondActivityTaskCompleted/Failed gRPC     │   │
│  └──────────────────────────────────────────────────────────┘   │
│                              │                                   │
│                              │ gRPC (native Temporal protocol)   │
│                              ▼                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    Temporal Server                        │   │
│  │  - Manages activity task queues                           │   │
│  │  - Handles retries, timeouts, heartbeats                  │   │
│  │  - Provides at-most-once delivery                         │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ HTTP (existing Conductor protocol)
                              ▼
               ┌──────────────────────────┐
               │  Existing Conductor      │
               │  Worker (unchanged)      │
               │                          │
               │  - Polls for tasks       │
               │  - Executes locally      │
               │  - Reports results       │
               └──────────────────────────┘
```

## Task Type to Task Queue Mapping

### Current Approach: 1:1 Mapping

Each Conductor task type maps directly to a Temporal task queue with the same name:

```
Conductor: GET /tasks/poll/send_email
    ↓
Temporal: PollActivityTaskQueue(taskQueue="send_email")
```

**Conductor workers poll in parallel per task type:**
```java
// From TaskRunnerConfigurer.java - one thread per task type
this.scheduledExecutorService = Executors.newScheduledThreadPool(workers.size());
workers.forEach(worker -> scheduledExecutorService.submit(() -> this.startWorker(worker)));
```

100 task types = 100 parallel HTTP polls = 100 parallel gRPC polls

**This works well because:**
1. Temporal is designed for many task queues - `PollActivityTaskQueue` is lightweight
2. No contention between different task types
3. Bridge is stateless - direct HTTP → gRPC translation
4. No filtering needed - each queue has exactly one task type

### Workflow Scheduling

When the workflow schedules a SIMPLE task, it uses the task type as the task queue:

```java
// In ConductorWorkflowImpl
for (Task task : tasksToSchedule) {
    if (task.getTaskType().equals("SIMPLE")) {
        String taskName = task.getTaskDefName();  // e.g., "send_email"

        ActivityOptions options = ActivityOptions.newBuilder()
            .setTaskQueue(taskName)  // Route to task-specific queue
            .setStartToCloseTimeout(Duration.ofSeconds(task.getResponseTimeoutSeconds()))
            .build();

        ActivityStub stub = Workflow.newUntypedActivityStub(options);
        stub.executeAsync(taskName, task.getInputData());
    }
}
```

## Implementation

### HTTP Polling Endpoint

```java
@RestController
@RequestMapping("/tasks")
public class ActivityPollingBridge {

    private final WorkflowServiceStubs serviceStubs;
    private final String namespace;

    /**
     * Poll for activity tasks - wraps Temporal's PollActivityTaskQueue.
     * Long-poll endpoint that blocks up to the specified timeout.
     */
    @GetMapping("/poll/{taskType}")
    public ResponseEntity<Task> pollTask(
            @PathVariable String taskType,
            @RequestParam(required = false) String workerid,
            @RequestParam(defaultValue = "30000") long timeout) {

        PollActivityTaskQueueRequest request = PollActivityTaskQueueRequest.newBuilder()
            .setNamespace(namespace)
            .setTaskQueue(TaskQueue.newBuilder()
                .setName(taskType)  // Task type = task queue name
                .build())
            .setIdentity(workerid != null ? workerid : "conductor-worker")
            .build();

        PollActivityTaskQueueResponse response = serviceStubs
            .blockingStub()
            .withDeadlineAfter(timeout, TimeUnit.MILLISECONDS)
            .pollActivityTaskQueue(request);

        if (response.getTaskToken().isEmpty()) {
            return ResponseEntity.noContent().build();
        }

        Task conductorTask = convertToTask(response, taskType);
        return ResponseEntity.ok(conductorTask);
    }

    /**
     * Complete activity task - wraps Temporal's RespondActivityTaskCompleted/Failed.
     */
    @PostMapping
    public ResponseEntity<Void> updateTask(@RequestBody TaskResult taskResult) {
        byte[] taskToken = Base64.getDecoder().decode(taskResult.getTaskId());

        if (taskResult.getStatus() == TaskStatus.COMPLETED) {
            RespondActivityTaskCompletedRequest request = RespondActivityTaskCompletedRequest.newBuilder()
                .setTaskToken(ByteString.copyFrom(taskToken))
                .setResult(convertToPayloads(taskResult.getOutputData()))
                .setIdentity(taskResult.getWorkerId())
                .build();
            serviceStubs.blockingStub().respondActivityTaskCompleted(request);
        } else if (taskResult.getStatus() == TaskStatus.FAILED) {
            RespondActivityTaskFailedRequest request = RespondActivityTaskFailedRequest.newBuilder()
                .setTaskToken(ByteString.copyFrom(taskToken))
                .setFailure(Failure.newBuilder()
                    .setMessage(taskResult.getReasonForIncompletion())
                    .build())
                .setIdentity(taskResult.getWorkerId())
                .build();
            serviceStubs.blockingStub().respondActivityTaskFailed(request);
        }

        return ResponseEntity.ok().build();
    }

    /**
     * Heartbeat activity - wraps Temporal's RecordActivityTaskHeartbeat.
     */
    @PostMapping("/{taskId}/ack")
    public ResponseEntity<Void> ackTask(
            @PathVariable String taskId,
            @RequestParam(required = false) String workerId) {

        byte[] taskToken = Base64.getDecoder().decode(taskId);

        RecordActivityTaskHeartbeatRequest request = RecordActivityTaskHeartbeatRequest.newBuilder()
            .setTaskToken(ByteString.copyFrom(taskToken))
            .setIdentity(workerId)
            .build();

        RecordActivityTaskHeartbeatResponse response =
            serviceStubs.blockingStub().recordActivityTaskHeartbeat(request);

        if (response.getCancelRequested()) {
            return ResponseEntity.status(HttpStatus.GONE).build();
        }

        return ResponseEntity.ok().build();
    }

    private Task convertToTask(PollActivityTaskQueueResponse response, String taskType) {
        Task task = new Task();
        task.setTaskId(Base64.getEncoder().encodeToString(
            response.getTaskToken().toByteArray()));
        task.setTaskType(taskType);
        task.setWorkflowInstanceId(response.getWorkflowExecution().getWorkflowId());
        task.setStatus(TaskStatus.IN_PROGRESS);
        task.setInputData(convertFromPayloads(response.getInput()));
        task.setResponseTimeoutSeconds((int) response.getStartToCloseTimeout().getSeconds());
        task.setCallbackAfterSeconds((int) response.getHeartbeatTimeout().getSeconds());
        return task;
    }
}
```

## Advantages

1. **No intermediate storage** - Temporal server handles all queuing
2. **Native retry/timeout handling** - Temporal manages failure scenarios
3. **At-most-once delivery** - Temporal's semantics preserved
4. **Heartbeat support** - Direct mapping to `RecordActivityTaskHeartbeat`
5. **Low latency** - Long-poll provides near-instant task delivery
6. **Stateless bridge** - No state to manage or sync
7. **Workers unchanged** - Existing Conductor workers work as-is

## Implementation Effort

| Component | Effort | Risk |
|-----------|--------|------|
| HTTP Polling Endpoint | 1-2 days | Low |
| HTTP Completion Endpoint | 1 day | Low |
| Heartbeat Endpoint | 0.5 days | Low |
| Payload Conversion | 1-2 days | Low |
| Testing | 2-3 days | Low |
| **Total** | **6-9 days** | **Low** |

---

## Future Enhancement: Worker Groups

The current 1:1 mapping creates one Temporal task queue per task type. For deployments with many task types (100+), this can be optimized by grouping task types that share workers.

### Concept

Workers that handle multiple task types can share a single Temporal task queue:

```
┌─────────────────────────────────────────────────────────────────┐
│  Task Type Registry                                             │
│                                                                 │
│  WorkerGroup "notification-workers" {                           │
│    taskTypes: [send_email, send_sms, send_push]                 │
│    taskQueue: "notification-tasks"                              │
│  }                                                              │
│                                                                 │
│  WorkerGroup "payment-workers" {                                │
│    taskTypes: [process_payment, refund_payment, verify_card]    │
│    taskQueue: "payment-tasks"                                   │
│  }                                                              │
└─────────────────────────────────────────────────────────────────┘
```

### How It Works

**Registration:** Workers register the task types they handle:
```
POST /worker-groups
{
  "name": "notification-workers",
  "taskTypes": ["send_email", "send_sms", "send_push"],
  "taskQueue": "notification-tasks"
}
```

**Workflow scheduling:** Uses registry to route to correct queue:
```java
String taskQueue = taskTypeRegistry.getTaskQueue(task.getTaskDefName());
ActivityOptions options = ActivityOptions.newBuilder()
    .setTaskQueue(taskQueue)
    .build();
```

**Polling:** Bridge maps task type to group's queue:
```
GET /tasks/poll/send_email
  → Lookup: send_email → notification-tasks
  → PollActivityTaskQueue(taskQueue="notification-tasks")
  → Returns any task from that queue
```

### Constraint

**Workers sharing a queue must handle ALL task types in that queue.**

If a worker polls for `send_email` but gets a `send_sms` task (same queue), it must be able to handle it. This is enforced by the registration - you can only add a task type to a group if all workers in that group support it.

### Benefits

- Reduces Temporal task queues from N (task types) to M (worker groups)
- More efficient resource utilization
- Aligns with how workers are typically deployed (one service handles related tasks)

### When to Use

- **Use 1:1 mapping (current)** for < 50 task types or when task types have independent workers
- **Use worker groups (future)** for 100+ task types where workers naturally group by domain

---

## Temporal gRPC APIs Used

### PollActivityTaskQueue

```protobuf
rpc PollActivityTaskQueue(PollActivityTaskQueueRequest)
    returns (PollActivityTaskQueueResponse);

message PollActivityTaskQueueRequest {
    string namespace = 1;
    TaskQueue task_queue = 2;
    string identity = 3;
}

message PollActivityTaskQueueResponse {
    bytes task_token = 1;
    ActivityType activity_type = 4;
    Payloads input = 5;
    WorkflowExecution workflow_execution = 7;
    google.protobuf.Duration start_to_close_timeout = 10;
    google.protobuf.Duration heartbeat_timeout = 11;
    int32 attempt = 12;
}
```

### RespondActivityTaskCompleted

```protobuf
rpc RespondActivityTaskCompleted(RespondActivityTaskCompletedRequest)
    returns (RespondActivityTaskCompletedResponse);

message RespondActivityTaskCompletedRequest {
    bytes task_token = 1;
    Payloads result = 2;
    string identity = 3;
}
```

### RecordActivityTaskHeartbeat

```protobuf
rpc RecordActivityTaskHeartbeat(RecordActivityTaskHeartbeatRequest)
    returns (RecordActivityTaskHeartbeatResponse);

message RecordActivityTaskHeartbeatRequest {
    bytes task_token = 1;
    Payloads details = 2;
    string identity = 3;
}

message RecordActivityTaskHeartbeatResponse {
    bool cancel_requested = 1;
}
```
