# Feasibility Analysis: HTTP Polling Bridge for Conductor Workers

## Overview

This document analyzes the feasibility of supporting existing Conductor workers that use HTTP polling through Temporal's activity gRPC interface, without requiring any changes to the workers themselves.

## Current Protocols

### Conductor HTTP Polling Protocol

Conductor workers use a pull-based HTTP polling model:

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

**Key Conductor Task Polling Response Fields:**
```json
{
  "taskId": "abc-123",
  "taskType": "send_email",
  "workflowInstanceId": "workflow-456",
  "inputData": { "email": "user@example.com" },
  "status": "IN_PROGRESS",
  "callbackAfterSeconds": 0,
  "responseTimeoutSeconds": 60
}
```

**Key Conductor TaskResult Fields:**
```json
{
  "taskId": "abc-123",
  "workflowInstanceId": "workflow-456",
  "status": "COMPLETED",
  "outputData": { "sent": true }
}
```

### Temporal Activity Protocol

Temporal uses a pull-based gRPC model with long-polling:

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

**Key insight:** Both protocols are fundamentally poll-based. The main difference is transport (HTTP vs gRPC) and message format.

## Bridge Architecture Options

### Option 1: Direct gRPC-to-HTTP Bridge (Recommended)

This approach wraps Temporal's native `PollActivityTaskQueue` gRPC call with an HTTP endpoint, providing a direct protocol translation without intermediate storage.

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
│  │  - Manages activity task queue                            │   │
│  │  - Handles retries, timeouts, heartbeats                  │   │
│  │  - Provides at-most-once delivery                         │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ HTTP (existing protocol)
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

#### Implementation

```java
@RestController
@RequestMapping("/tasks")
public class ActivityPollingBridge {

    private final WorkflowServiceStubs serviceStubs;
    private final String namespace;
    private final ObjectMapper objectMapper;

    /**
     * Poll for activity tasks - wraps Temporal's PollActivityTaskQueue.
     * This is a long-poll endpoint that blocks up to the specified timeout.
     */
    @GetMapping("/poll/{taskType}")
    public ResponseEntity<Task> pollTask(
            @PathVariable String taskType,
            @RequestParam(required = false) String workerid,
            @RequestParam(defaultValue = "30000") long timeout) {

        // Build Temporal poll request
        // taskType directly maps to Temporal task queue name (1:1)
        PollActivityTaskQueueRequest request = PollActivityTaskQueueRequest.newBuilder()
            .setNamespace(namespace)
            .setTaskQueue(TaskQueue.newBuilder()
                .setName(taskType)  // Task type IS the task queue name
                .build())
            .setIdentity(workerid != null ? workerid : "conductor-worker")
            .build();

        // Long-poll Temporal (blocks up to server's long-poll timeout, typically 60s)
        PollActivityTaskQueueResponse response = serviceStubs
            .blockingStub()
            .withDeadlineAfter(timeout, TimeUnit.MILLISECONDS)
            .pollActivityTaskQueue(request);

        // Empty response = no task available
        if (response.getTaskToken().isEmpty()) {
            return ResponseEntity.noContent().build();
        }

        // Convert to Conductor Task format
        Task conductorTask = convertToTask(response, taskType);
        return ResponseEntity.ok(conductorTask);
    }

    /**
     * Complete activity task - wraps Temporal's RespondActivityTaskCompleted/Failed.
     */
    @PostMapping
    public ResponseEntity<Void> updateTask(@RequestBody TaskResult taskResult) {
        // taskId contains the base64-encoded Temporal taskToken
        byte[] taskToken = Base64.getDecoder().decode(taskResult.getTaskId());

        if (taskResult.getStatus() == TaskStatus.COMPLETED) {
            // Convert output to Temporal Payloads
            Payloads result = convertToPayloads(taskResult.getOutputData());

            RespondActivityTaskCompletedRequest request = RespondActivityTaskCompletedRequest.newBuilder()
                .setTaskToken(ByteString.copyFrom(taskToken))
                .setResult(result)
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
     * Conductor uses callbackAfterSeconds, we map to Temporal heartbeats.
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
            // Activity was cancelled - worker should stop
            return ResponseEntity.status(HttpStatus.GONE).build();
        }

        return ResponseEntity.ok().build();
    }

    private Task convertToTask(PollActivityTaskQueueResponse response, String taskType) {
        Task task = new Task();

        // Use base64-encoded taskToken as taskId
        // Worker will pass this back when completing
        task.setTaskId(Base64.getEncoder().encodeToString(
            response.getTaskToken().toByteArray()));

        task.setTaskType(taskType);
        task.setWorkflowInstanceId(response.getWorkflowExecution().getWorkflowId());
        task.setStatus(TaskStatus.IN_PROGRESS);

        // Convert Temporal Payloads to Conductor inputData
        task.setInputData(convertFromPayloads(response.getInput()));

        // Map timeouts
        task.setResponseTimeoutSeconds(
            (int) response.getStartToCloseTimeout().getSeconds());
        task.setCallbackAfterSeconds(
            (int) response.getHeartbeatTimeout().getSeconds());

        return task;
    }
}
```

#### Advantages

1. **No intermediate storage** - Temporal server handles all queuing
2. **Native retry/timeout handling** - Temporal manages all failure scenarios
3. **At-most-once delivery** - Temporal's semantics are preserved
4. **Heartbeat support** - Direct mapping to `RecordActivityTaskHeartbeat`
5. **Low latency** - Long-poll provides near-instant task delivery
6. **Stateless bridge** - No state to manage or sync

#### Challenges

| Challenge | Solution | Complexity |
|-----------|----------|------------|
| Task type → Task queue mapping | 1:1 mapping: task type = task queue name | Low |
| Payload format conversion | JSON ↔ Temporal Payloads | Low |
| Workflow scheduling | Schedule activities to task-type-named queues | Low |

### Option 2: Async Activity Completion Bridge (Alternative)

This approach uses Temporal's async activity completion feature with intermediate storage.

```
┌─────────────────────────────────────────────────────────────────┐
│                     Temporal Conductor Server                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐    ┌──────────────────┐    ┌───────────────┐ │
│  │  Temporal    │───►│ Bridge Activity  │───►│ Pending Task  │ │
│  │  Workflow    │    │ (doNotComplete)  │    │ Store         │ │
│  └──────────────┘    └──────────────────┘    └───────┬───────┘ │
│                                                       │         │
│  ┌──────────────┐    ┌──────────────────┐           │         │
│  │  Activity    │◄───│ Task Completion  │◄──────────┤         │
│  │  Completion  │    │ Handler          │           │         │
│  │  Client      │    └──────────────────┘           │         │
│  └──────────────┘                                    │         │
│                                                       │         │
│  ┌────────────────────────────────────────────────────┴──────┐ │
│  │              HTTP Polling Endpoints                        │ │
│  │  GET  /tasks/poll/{taskType}  - Returns pending task       │ │
│  │  POST /tasks                  - Completes task             │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

This approach has higher complexity due to the need for:
- Intermediate task storage
- Task cleanup/expiration logic
- Coordination between workflow activities and HTTP endpoints

**Use this approach if:** You need to support workflows that don't know about Conductor task types at definition time.

## Key Integration Point: Task Type to Task Queue Mapping

Conductor workers poll by **task type** (e.g., `send_email`, `process_payment`).
Temporal workers poll by **task queue name**, and the activity type is returned in the response.

**Key insight:** `PollActivityTaskQueue` does NOT require knowing activity types in advance - it only needs the task queue name. This simplifies the bridge significantly.

### Mapping Strategy: Task Type = Task Queue Name

The simplest mapping is 1:1: each Conductor task type becomes a Temporal task queue:

```
Conductor: GET /tasks/poll/send_email
    ↓
Temporal: PollActivityTaskQueue(taskQueue="send_email")
```

```java
@GetMapping("/poll/{taskType}")
public ResponseEntity<Task> pollTask(@PathVariable String taskType, ...) {

    PollActivityTaskQueueRequest request = PollActivityTaskQueueRequest.newBuilder()
        .setNamespace(namespace)
        .setTaskQueue(TaskQueue.newBuilder()
            .setName(taskType)  // Task type IS the task queue name
            .build())
        .setIdentity(workerId)
        .build();

    // ...
}
```

### Workflow Side: Schedule Activities to Task Type Queue

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
        stub.executeAsync("execute", task.getInputData());
    }
}
```

### No Pre-Registration Needed

The bridge does NOT require:
- ❌ Activity interfaces defined in advance
- ❌ Activity implementations registered with worker
- ❌ Knowledge of task types at deployment time

It only requires:
- ✅ Conductor worker polling the HTTP endpoint
- ✅ Workflow scheduling activities to the correct task queue

## Comparison Matrix

| Aspect | Option 1: gRPC Bridge | Option 2: Async Completion | Native SDK (Plan 14) |
|--------|----------------------|---------------------------|---------------------|
| Worker Changes | None | None | None (same annotations) |
| Latency | ~0ms (long-poll) | polling interval | ~0ms (push) |
| Complexity | Low-Medium | High | Low |
| Intermediate Storage | None | Required | None |
| Heartbeating | Direct gRPC | Must proxy | Automatic |
| Temporal Retries | Automatic | Must handle | Automatic |
| Scalability | High (Temporal handles) | Medium (central store) | High |
| Debugging | Straightforward | Complex (async) | Easy |

## Feasibility Assessment

### Option 1 (gRPC Bridge): Highly Feasible

**Technical Requirements:**
- ✅ `PollActivityTaskQueue` gRPC is a public Temporal API
- ✅ Long-polling maps naturally to HTTP long-polling
- ✅ Task tokens can be passed as opaque identifiers
- ✅ `RespondActivityTaskCompleted` provides completion
- ✅ `RecordActivityTaskHeartbeat` provides heartbeat support

**Implementation Effort:**

| Component | Effort | Risk |
|-----------|--------|------|
| HTTP Polling Endpoint | 1-2 days | Low |
| HTTP Completion Endpoint | 1 day | Low |
| Heartbeat Endpoint | 0.5 days | Low |
| Payload Conversion | 1-2 days | Low |
| Activity Registration Strategy | 1-2 days | Medium |
| Testing | 2-3 days | Low |
| **Total** | **7-10 days** | **Low-Medium** |

### Option 2 (Async Completion): Feasible but Complex

Same 10-15 days as in original analysis, with higher risk.

## Recommendation

### For Temporal Conductor:

**Implement Option 1 (gRPC Bridge)** if HTTP worker support is needed.

Reasons:
1. **Simpler architecture** - No intermediate storage
2. **Temporal-native** - Uses Temporal's own polling mechanism
3. **Lower risk** - Temporal handles all the hard parts (queuing, retries, timeouts)
4. **Lower maintenance** - Stateless bridge is easier to operate

### Implementation Priority:

1. **First**: Native SDK (Plan 14) - For new/redeployable Java workers
2. **Second**: gRPC Bridge (Option 1) - For legacy HTTP workers that cannot be touched
3. **Skip**: Async Completion (Option 2) - Too complex for the value

### Architecture Decision

```
                    ┌─────────────────────────────────┐
                    │     Conductor Task Types         │
                    └─────────────────────────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │                               │
                    ▼                               ▼
        ┌───────────────────┐           ┌───────────────────┐
        │   Java Workers    │           │  HTTP Workers     │
        │  (@WorkerTask)    │           │  (Legacy/Other)   │
        └───────────────────┘           └───────────────────┘
                    │                               │
                    ▼                               ▼
        ┌───────────────────┐           ┌───────────────────┐
        │  Native SDK       │           │  gRPC-HTTP Bridge │
        │  (Plan 14)        │           │  (This Plan)      │
        └───────────────────┘           └───────────────────┘
                    │                               │
                    └───────────────┬───────────────┘
                                    ▼
                    ┌─────────────────────────────────┐
                    │       Temporal Server           │
                    │  (Activity Task Queues)         │
                    └─────────────────────────────────┘
```

Both approaches complement each other:
- Native SDK for best performance and developer experience
- gRPC Bridge for backward compatibility with existing HTTP workers

## Appendix: Temporal gRPC APIs Used

### PollActivityTaskQueue

```protobuf
rpc PollActivityTaskQueue(PollActivityTaskQueueRequest)
    returns (PollActivityTaskQueueResponse);

message PollActivityTaskQueueRequest {
    string namespace = 1;
    TaskQueue task_queue = 2;
    string identity = 3;
    WorkerVersionCapabilities worker_version_capabilities = 4;
}

message PollActivityTaskQueueResponse {
    bytes task_token = 1;
    string activity_id = 3;
    ActivityType activity_type = 4;
    Payloads input = 5;
    WorkflowExecution workflow_execution = 7;
    google.protobuf.Duration schedule_to_close_timeout = 9;
    google.protobuf.Duration start_to_close_timeout = 10;
    google.protobuf.Duration heartbeat_timeout = 11;
    int32 attempt = 12;
    // ... more fields
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
