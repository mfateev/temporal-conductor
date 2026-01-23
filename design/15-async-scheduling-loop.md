# Async Scheduling Loop Refactoring

## Problem

Current implementation uses blocking patterns:
1. `activities.executeTask()` blocks until activity completes
2. `Workflow.await(Duration.ofSeconds(1), ...)` polls every second
3. `Promise.allOf(promises).get()` blocks until ALL tasks complete
4. WAIT tasks would require polling to check timeout

## Solution

Use event-driven architecture with `Promise.anyOf()`:

```
┌─────────────────────────────────────────────────────────────┐
│                    Main Scheduling Loop                      │
├─────────────────────────────────────────────────────────────┤
│  1. Call DeciderService.decide()                            │
│  2. Start new tasks asynchronously (return Promises)        │
│  3. Promise.anyOf(allPendingPromises) - wait for ANY event  │
│  4. Process completed tasks, update workflowModel           │
│  5. Repeat until workflow terminal                          │
└─────────────────────────────────────────────────────────────┘
```

## New State Fields

```java
// Track pending work
private final Map<String, Promise<TaskExecutionResult>> activityPromises = new HashMap<>();
private final Map<String, Promise<Void>> timerPromises = new HashMap<>();

// Track which tasks are waiting for completion
private final Set<String> pendingTaskIds = new HashSet<>();
```

## Key Changes

### 1. Start Worker Task Async

**Before:**
```java
private void executeWorkerTask(TaskModel task) {
    task.setStatus(IN_PROGRESS);
    TaskExecutionResult result = activities.executeTask(...);  // BLOCKS
    task.setStatus(COMPLETED);
}
```

**After:**
```java
private void startWorkerTaskAsync(TaskModel task) {
    task.setStatus(IN_PROGRESS);
    task.setStartTime(Workflow.currentTimeMillis());

    Promise<TaskExecutionResult> promise = Async.function(
        activities::executeTask,
        task.getTaskDefName(),
        task.getReferenceTaskName(),
        task.getInputData() != null ? task.getInputData() : Collections.emptyMap()
    );

    activityPromises.put(task.getTaskId(), promise);
    pendingTaskIds.add(task.getTaskId());
}
```

### 2. Handle WAIT Task with Timer

```java
private void startWaitTask(TaskModel task) {
    task.setStatus(IN_PROGRESS);
    task.setStartTime(Workflow.currentTimeMillis());

    long waitUntil = task.getWaitTimeout();
    if (waitUntil > 0) {
        long sleepMs = waitUntil - Workflow.currentTimeMillis();
        if (sleepMs > 0) {
            Promise<Void> timerPromise = Workflow.newTimer(Duration.ofMillis(sleepMs));
            timerPromises.put(task.getTaskId(), timerPromise);
            pendingTaskIds.add(task.getTaskId());
        } else {
            // Already past timeout
            task.setStatus(COMPLETED);
            task.setEndTime(Workflow.currentTimeMillis());
        }
    }
    // If waitUntil == 0, wait for signal (no timer)
}
```

### 3. New Main Loop

```java
private void runSchedulingLoop() {
    while (!workflowModel.getStatus().isTerminal()) {

        // Handle pause
        if (isPaused) {
            Workflow.await(() -> !isPaused || hasTerminateSignal);
            continue;
        }

        // Get scheduling decision
        DeciderOutcome outcome = deciderService.decide(workflowModel);

        if (DeciderOutcomeAccessor.isComplete(outcome)) {
            completeWorkflow();
            break;
        }

        // Apply task updates
        for (TaskModel task : DeciderOutcomeAccessor.getTasksToBeUpdated(outcome)) {
            updateTaskInWorkflow(task);
        }

        // Schedule new tasks asynchronously
        List<TaskModel> tasksToSchedule = DeciderOutcomeAccessor.getTasksToBeScheduled(outcome);
        for (TaskModel task : tasksToSchedule) {
            task.setWorkflowInstanceId(workflowModel.getWorkflowId());
            workflowModel.getTasks().add(task);
            scheduleTaskAsync(task);
        }

        // If nothing pending, check if workflow should complete
        if (pendingTaskIds.isEmpty() && pendingSignals.isEmpty()) {
            if (shouldCompleteWorkflow()) {
                completeWorkflow();
                break;
            }
            // Nothing to wait for but workflow not complete - shouldn't happen
            logger.warn("No pending work but workflow not complete");
            continue;
        }

        // Build list of all promises to wait on
        List<Promise<?>> allPromises = new ArrayList<>();
        allPromises.addAll(activityPromises.values());
        allPromises.addAll(timerPromises.values());

        // Wait for ANY event (activity, timer, or signal)
        // Signals wake up await() automatically via Temporal's mechanics
        Workflow.await(() ->
            anyPromiseCompleted() ||
            !pendingSignals.isEmpty() ||
            isPaused
        );

        // Process completed activities
        processCompletedActivities();

        // Process completed timers (WAIT tasks)
        processCompletedTimers();

        // Process signals
        if (!pendingSignals.isEmpty()) {
            processSignals();
        }

        // Re-evaluate IN_PROGRESS system tasks (JOIN, DO_WHILE)
        reEvaluateInProgressSystemTasks();
    }
}

private boolean anyPromiseCompleted() {
    for (Promise<?> p : activityPromises.values()) {
        if (p.isCompleted()) return true;
    }
    for (Promise<?> p : timerPromises.values()) {
        if (p.isCompleted()) return true;
    }
    return false;
}
```

### 4. Process Completed Activities

```java
private void processCompletedActivities() {
    List<String> completedTaskIds = new ArrayList<>();

    for (Map.Entry<String, Promise<TaskExecutionResult>> entry : activityPromises.entrySet()) {
        String taskId = entry.getKey();
        Promise<TaskExecutionResult> promise = entry.getValue();

        if (promise.isCompleted()) {
            TaskModel task = getTaskById(taskId);
            try {
                TaskExecutionResult result = promise.get();
                task.setOutputData(result.getOutput());
                if ("COMPLETED".equals(result.getStatus())) {
                    task.setStatus(TaskModel.Status.COMPLETED);
                } else {
                    task.setStatus(TaskModel.Status.FAILED);
                    task.setReasonForIncompletion(result.getFailureReason());
                }
            } catch (Exception e) {
                task.setStatus(TaskModel.Status.FAILED);
                task.setReasonForIncompletion(e.getMessage());
            }
            task.setEndTime(Workflow.currentTimeMillis());
            taskOutputs.put(task.getReferenceTaskName(), task.getOutputData());

            completedTaskIds.add(taskId);
            pendingTaskIds.remove(taskId);
        }
    }

    for (String taskId : completedTaskIds) {
        activityPromises.remove(taskId);
    }
}
```

### 5. Process Completed Timers

```java
private void processCompletedTimers() {
    List<String> completedTaskIds = new ArrayList<>();

    for (Map.Entry<String, Promise<Void>> entry : timerPromises.entrySet()) {
        String taskId = entry.getKey();
        Promise<Void> promise = entry.getValue();

        if (promise.isCompleted()) {
            TaskModel task = getTaskById(taskId);
            task.setStatus(TaskModel.Status.COMPLETED);
            task.setEndTime(Workflow.currentTimeMillis());

            completedTaskIds.add(taskId);
            pendingTaskIds.remove(taskId);
        }
    }

    for (String taskId : completedTaskIds) {
        timerPromises.remove(taskId);
    }
}
```

### 6. Schedule Task Async (Router)

```java
private void scheduleTaskAsync(TaskModel task) {
    String taskType = task.getTaskType();

    if (TaskType.WAIT.name().equals(taskType)) {
        startWaitTask(task);
    } else if (isSystemTask(task)) {
        // System tasks execute synchronously (FORK, SWITCH, SET_VARIABLE, etc.)
        // They don't block - they just manipulate workflow state
        executeSystemTask(task);

        // Async system tasks (JOIN, DO_WHILE) go IN_PROGRESS
        // They'll be re-evaluated when their dependencies complete
    } else {
        // Worker tasks (SIMPLE, HTTP) run as activities
        startWorkerTaskAsync(task);
    }
}
```

## Events That Wake Up the Loop

1. **Activity completion** - Promise completes
2. **Timer fires** - Timer promise completes
3. **Signal received** - `pendingSignals` becomes non-empty, wakes `Workflow.await()`
4. **Pause/Resume** - `isPaused` changes, wakes `Workflow.await()`

## Benefits

1. **No polling** - Pure event-driven
2. **No MAX_ITERATIONS** - Loop runs as long as needed
3. **Efficient** - Only wakes when something happens
4. **Correct semantics** - Matches how Temporal workflows should work
5. **WAIT tasks work** - Use durable timers instead of polling

## Migration Steps

1. Add new state fields (activityPromises, timerPromises, pendingTaskIds)
2. Rename `executeWorkerTask` → `startWorkerTaskAsync` (returns void, stores promise)
3. Add `startWaitTask` for WAIT task handling
4. Add `processCompletedActivities` and `processCompletedTimers`
5. Rewrite `runSchedulingLoop` to use event-driven pattern
6. Remove `waitForSignalsOrChanges` method
7. Remove `MAX_ITERATIONS` constant
8. Update `executeTasksInParallel` to not block (just start all tasks)
9. Add helper `getTaskById` and `anyPromiseCompleted`
