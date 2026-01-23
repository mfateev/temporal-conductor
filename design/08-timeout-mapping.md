# Conductor to Temporal Timeout Mapping

## Summary

**All Conductor timeout semantics can be supported using native Temporal features**, with one minor exception: `ALERT_ONLY` timeout policy requires external monitoring integration (which is the same in both systems).

## Key Finding: Timeouts Can Be Disabled Without Forking Conductor

**Critical discovery**: All Conductor timeout checks have early-return conditions that skip execution when timeout values are set to 0. This means we can disable Conductor's timeout checking entirely via configuration, without modifying the Conductor codebase.

### DeciderService Timeout Skip Conditions

| Method | Skip Condition | Source Line |
|--------|----------------|-------------|
| `checkWorkflowTimeout()` | `workflowDef.getTimeoutSeconds() <= 0` | DeciderService.java:659 |
| `checkTaskTimeout()` | `taskDef.getTimeoutSeconds() <= 0` | DeciderService.java:707 |
| `checkTaskPollTimeout()` | `taskDef.getPollTimeoutSeconds() == null \|\| <= 0` | DeciderService.java:741-742 |
| `isResponseTimedOut()` | `taskDef.getResponseTimeoutSeconds() == 0` | DeciderService.java:817 |

### Implementation

Preprocess workflow definitions before passing to DeciderService:

```kotlin
fun disableConductorTimeouts(workflowDef: WorkflowDef): WorkflowDef {
    // Disable workflow-level timeout (avoids System.currentTimeMillis() call)
    workflowDef.timeoutSeconds = 0

    // Disable task-level timeouts on all tasks
    workflowDef.tasks.forEach { task ->
        task.taskDefinition?.let { taskDef ->
            taskDef.timeoutSeconds = 0
            taskDef.pollTimeoutSeconds = 0
            taskDef.responseTimeoutSeconds = 0
        }
    }
    return workflowDef
}
```

This approach:
1. **Avoids non-determinism** - No `System.currentTimeMillis()` calls during workflow replay
2. **No fork required** - Uses standard Conductor libraries unmodified
3. **Preserves semantics** - Equivalent timeouts configured via Temporal instead

## Conductor Timeout Types

### Task-Level Timeouts (TaskDef)

| Conductor Timeout | Description | Temporal Equivalent | Notes |
|-------------------|-------------|---------------------|-------|
| `timeoutSeconds` | Task execution timeout (per attempt) | `startToCloseTimeout` | ✅ Direct mapping |
| `responseTimeoutSeconds` | Time for worker to respond after polling | `heartbeatTimeout` | ✅ Worker must heartbeat within this period |
| `pollTimeoutSeconds` | Time after scheduling before task is polled | `scheduleToStartTimeout` | ✅ Time from scheduled to worker pickup |
| `totalTimeoutSeconds` | Total timeout across all retries | `scheduleToCloseTimeout` | ✅ Total time including all retries |

### Workflow-Level Timeouts (WorkflowDef)

| Conductor Timeout | Description | Temporal Equivalent | Notes |
|-------------------|-------------|---------------------|-------|
| `timeoutSeconds` | Workflow execution timeout | `workflowExecutionTimeout` | ✅ Direct mapping |

### Timeout Policies (TaskDef.TimeoutPolicy)

| Conductor Policy | Behavior | Temporal Equivalent | Notes |
|------------------|----------|---------------------|-------|
| `RETRY` | Retry the task on timeout | Automatic with `RetryPolicy` | ✅ Configure `maximumAttempts` |
| `TIME_OUT_WF` | Fail workflow on timeout | Activity failure propagates to workflow | ✅ Don't catch activity exception |
| `ALERT_ONLY` | Alert but don't take action | External monitoring | ⚠️ Use visibility API + alerts |

### Timeout Policies (WorkflowDef.TimeoutPolicy)

| Conductor Policy | Behavior | Temporal Equivalent | Notes |
|------------------|----------|---------------------|-------|
| `TIME_OUT_WF` | Fail workflow on timeout | `workflowExecutionTimeout` | ✅ Direct mapping |
| `ALERT_ONLY` | Alert but don't fail | External monitoring | ⚠️ Monitor via visibility API |

## Detailed Mapping

### 1. Task Execution Timeout → `startToCloseTimeout`

**Conductor:**
```json
{
  "name": "my-task",
  "timeoutSeconds": 300
}
```

**Temporal:**
```kotlin
val activityOptions = ActivityOptions.newBuilder()
    .setStartToCloseTimeout(Duration.ofSeconds(300))
    .build()
```

### 2. Response Timeout → `heartbeatTimeout`

**Conductor's `responseTimeoutSeconds`:** Time for worker to respond after picking up task.

**Temporal's `heartbeatTimeout`:** Maximum time between heartbeats. Worker must periodically call `Activity.getExecutionContext().heartbeat()`.

```kotlin
val activityOptions = ActivityOptions.newBuilder()
    .setHeartbeatTimeout(Duration.ofSeconds(60))
    .build()
```

Note: For short tasks, Temporal doesn't require heartbeats. The `startToCloseTimeout` handles overall execution.

### 3. Poll Timeout → `scheduleToStartTimeout`

**Conductor's `pollTimeoutSeconds`:** Time for task to be picked up after scheduling.

**Temporal's `scheduleToStartTimeout`:** Maximum time from when activity is scheduled to when a worker starts executing it.

```kotlin
val activityOptions = ActivityOptions.newBuilder()
    .setScheduleToStartTimeout(Duration.ofSeconds(120))
    .build()
```

This catches scenarios where no workers are available.

### 4. Total Timeout → `scheduleToCloseTimeout`

**Conductor's `totalTimeoutSeconds`:** Total time including all retries.

**Temporal's `scheduleToCloseTimeout`:** Total time from scheduling to completion, including all retry attempts.

```kotlin
val activityOptions = ActivityOptions.newBuilder()
    .setScheduleToCloseTimeout(Duration.ofMinutes(10))
    .setRetryOptions(RetryOptions.newBuilder()
        .setMaximumAttempts(3)
        .build())
    .build()
```

### 5. Workflow Timeout → `workflowExecutionTimeout`

**Conductor:**
```json
{
  "name": "my-workflow",
  "timeoutSeconds": 3600
}
```

**Temporal:**
```kotlin
val workflowOptions = WorkflowOptions.newBuilder()
    .setWorkflowExecutionTimeout(Duration.ofHours(1))
    .build()
```

### 6. Retry Policy

**Conductor:**
```json
{
  "name": "my-task",
  "retryCount": 3,
  "retryLogic": "EXPONENTIAL_BACKOFF",
  "retryDelaySeconds": 10,
  "backoffScaleFactor": 2
}
```

**Temporal:**
```kotlin
val retryOptions = RetryOptions.newBuilder()
    .setMaximumAttempts(3)
    .setInitialInterval(Duration.ofSeconds(10))
    .setBackoffCoefficient(2.0)
    .build()
```

## ALERT_ONLY Policy

Both Conductor and Temporal handle `ALERT_ONLY` through external monitoring rather than built-in workflow features.

**In Conductor:** The workflow/task continues running, and an event is published for monitoring systems.

**In Temporal:**
1. Use visibility API to query long-running workflows/activities
2. Set up alerts based on workflow search attributes
3. Use interceptors to emit metrics

```kotlin
// Temporal: Monitor via visibility API
val workflows = client.listWorkflowExecutions(
    ListWorkflowExecutionsRequest.newBuilder()
        .setQuery("ExecutionStatus='Running' AND StartTime < '2026-01-18T00:00:00Z'")
        .build()
)
```

## Conversion Function

Here's how the translator would map TaskDef timeouts to Temporal ActivityOptions:

```kotlin
fun convertTimeouts(taskDef: TaskDef): ActivityOptions {
    val builder = ActivityOptions.newBuilder()

    // Per-attempt execution timeout
    if (taskDef.timeoutSeconds > 0) {
        builder.setStartToCloseTimeout(Duration.ofSeconds(taskDef.timeoutSeconds))
    }

    // Total timeout across retries
    if (taskDef.totalTimeoutSeconds > 0) {
        builder.setScheduleToCloseTimeout(Duration.ofSeconds(taskDef.totalTimeoutSeconds))
    }

    // Time to be picked up by worker
    taskDef.pollTimeoutSeconds?.let {
        if (it > 0) {
            builder.setScheduleToStartTimeout(Duration.ofSeconds(it.toLong()))
        }
    }

    // Response/heartbeat timeout
    if (taskDef.responseTimeoutSeconds > 0) {
        builder.setHeartbeatTimeout(Duration.ofSeconds(taskDef.responseTimeoutSeconds))
    }

    // Retry policy
    if (taskDef.timeoutPolicy == TaskDef.TimeoutPolicy.RETRY && taskDef.retryCount > 0) {
        builder.setRetryOptions(RetryOptions.newBuilder()
            .setMaximumAttempts(taskDef.retryCount)
            .setInitialInterval(Duration.ofSeconds(taskDef.retryDelaySeconds.toLong()))
            .setBackoffCoefficient(when (taskDef.retryLogic) {
                TaskDef.RetryLogic.EXPONENTIAL_BACKOFF -> taskDef.backoffScaleFactor?.toDouble() ?: 2.0
                TaskDef.RetryLogic.LINEAR_BACKOFF -> 1.0
                else -> 1.0
            })
            .build())
    }

    return builder.build()
}
```

## Conclusion

| Feature | Supported | Notes |
|---------|-----------|-------|
| Task execution timeout | ✅ Yes | `startToCloseTimeout` |
| Response timeout | ✅ Yes | `heartbeatTimeout` |
| Poll timeout | ✅ Yes | `scheduleToStartTimeout` |
| Total timeout | ✅ Yes | `scheduleToCloseTimeout` |
| Workflow timeout | ✅ Yes | `workflowExecutionTimeout` |
| Retry on timeout | ✅ Yes | `RetryOptions` |
| Fail workflow on timeout | ✅ Yes | Activity exception propagation |
| Alert only | ⚠️ External | Same as Conductor - use monitoring |

**Recommendation:** Disable Conductor timeout checks (set to 0) and configure equivalent Temporal timeouts instead. This avoids the non-determinism issue with `System.currentTimeMillis()` in DeciderService while maintaining all timeout semantics.
