# HTTP Polling Bridge for Conductor Workers

## Current Implementation: Activity-Based SIMPLE Tasks

**Currently, only activity-based SIMPLE task execution is supported.** SIMPLE tasks are executed internally via Temporal activities (`TaskExecutionActivitiesImpl`), which runs on the same task queue as the workflow.

```
┌─────────────────────────────────────────────────────────────────┐
│                  Temporal Conductor Server                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              ConductorWorkflowImpl                        │   │
│  │                                                           │   │
│  │  Schedules SIMPLE tasks as Temporal activities            │   │
│  │  on the default "conductor-workflows" task queue          │   │
│  └──────────────────────────────────────────────────────────┘   │
│                              │                                   │
│                              ▼                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │         TaskExecutionActivitiesImpl (DynamicActivity)     │   │
│  │                                                           │   │
│  │  - Receives activity invocations for all task types       │   │
│  │  - Executes task logic internally                         │   │
│  │  - Returns TaskExecutionResult to workflow                │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

This approach requires implementing task logic as Temporal activities within the Conductor server.

---

## Future Enhancement: HTTP Polling Bridge

> **Status: NOT IMPLEMENTED**
>
> The following describes a future enhancement to support existing Conductor workers
> that use HTTP polling, without requiring any changes to those workers.

### Overview

This enhancement would allow existing Conductor workers to poll for and complete tasks via the standard Conductor HTTP API, while Temporal manages the underlying task queues and execution guarantees.

### Protocol Comparison

**Conductor HTTP Polling:**
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

**Temporal Activity Polling:**
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

### Proposed Architecture

The bridge would wrap Temporal's `PollActivityTaskQueue` gRPC with HTTP endpoints:

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

### Task Type to Task Queue Mapping

Each Conductor task type would map directly to a Temporal task queue with the same name:

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

### Mode Differentiation

To support both internal activities and HTTP bridge, tasks would need to opt-in to HTTP bridge mode. Options include:

1. **`asyncComplete: true`** on the task spec - semantically appropriate ("wait for external completion")
2. **`inputParameters._httpBridge: true`** - explicit opt-in via input
3. **Global config flag** - all SIMPLE tasks use HTTP bridge

When HTTP bridge mode is enabled for a task, the workflow would route it to a task-type-specific queue:

```java
// In ConductorWorkflowImpl.buildActivityOptions()
if (httpBridgeEnabled) {
    optionsBuilder.setTaskQueue(task.getTaskDefName());  // e.g., "send_email"
}
```

### Implementation Sketch

```java
@RestController
@RequestMapping("/tasks")
public class ActivityPollingBridge {

    private final WorkflowServiceStubs serviceStubs;
    private final String namespace;

    @GetMapping("/poll/{taskType}")
    public ResponseEntity<Task> pollTask(
            @PathVariable String taskType,
            @RequestParam(required = false) String workerid,
            @RequestParam(defaultValue = "30000") long timeout) {

        PollActivityTaskQueueRequest request = PollActivityTaskQueueRequest.newBuilder()
            .setNamespace(namespace)
            .setTaskQueue(TaskQueue.newBuilder().setName(taskType).build())
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
}
```

### Advantages

1. **No intermediate storage** - Temporal server handles all queuing
2. **Native retry/timeout handling** - Temporal manages failure scenarios
3. **At-most-once delivery** - Temporal's semantics preserved
4. **Heartbeat support** - Direct mapping to `RecordActivityTaskHeartbeat`
5. **Low latency** - Long-poll provides near-instant task delivery
6. **Stateless bridge** - No state to manage or sync
7. **Workers unchanged** - Existing Conductor workers work as-is

### Estimated Implementation Effort

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

For deployments with many task types (100+), the 1:1 mapping can be optimized by grouping task types that share workers.

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

### Constraint

**Workers sharing a queue must handle ALL task types in that queue.**

### When to Use

- **Use 1:1 mapping** for < 50 task types or when task types have independent workers
- **Use worker groups** for 100+ task types where workers naturally group by domain

---

## Temporal gRPC APIs Reference

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
