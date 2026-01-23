# Core Architecture

## Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                         Temporal Worker                               │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │                    ConductorWorkflow                             │ │
│  │                                                                  │ │
│  │  Input: WorkflowDef (JSON) + workflow input                     │ │
│  │                                                                  │ │
│  │  ┌────────────────────────────────────────────────────────────┐ │ │
│  │  │              In-Memory State & Adapters                     │ │ │
│  │  │                                                             │ │ │
│  │  │   InMemoryMetadataDAO        WorkflowModel                 │ │ │
│  │  │   ┌─────────────────┐        ┌──────────────────────┐     │ │ │
│  │  │   │ taskDefs: Map   │        │ tasks: List<TaskModel>│     │ │ │
│  │  │   │ workflowDefs:Map│        │ variables: Map        │     │ │ │
│  │  │   └─────────────────┘        └──────────────────────┘     │ │ │
│  │  │                                                             │ │ │
│  │  │   InMemoryWorkflowExecutor   InMemoryExecutionDAOFacade   │ │ │
│  │  │   ┌─────────────────┐        ┌──────────────────────┐     │ │ │
│  │  │   │scheduleNext     │        │ updateWorkflow: no-op│     │ │ │
│  │  │   │  Iteration()    │        │ removeTask: no-op    │     │ │ │
│  │  │   └─────────────────┘        └──────────────────────┘     │ │ │
│  │  │                                                             │ │ │
│  │  └────────────────────────────────────────────────────────────┘ │ │
│  │                              │                                   │ │
│  │                              ▼                                   │ │
│  │  ┌────────────────────────────────────────────────────────────┐ │ │
│  │  │           DeciderService (from Conductor)                   │ │ │
│  │  │                                                             │ │ │
│  │  │   - Expression evaluation (ParametersUtils)                 │ │ │
│  │  │   - Task scheduling decisions                               │ │ │
│  │  │   - decide() → DeciderOutcome (tasks to schedule)          │ │ │
│  │  │                                                             │ │ │
│  │  └────────────────────────────────────────────────────────────┘ │ │
│  │                              │                                   │ │
│  │                              ▼                                   │ │
│  │  ┌────────────────────────────────────────────────────────────┐ │ │
│  │  │              SystemTaskExecutor                             │ │ │
│  │  │                                                             │ │ │
│  │  │   Delegates ALL system tasks to Conductor's implementations │ │ │
│  │  │   - Fork, Join, Switch, Decision, Terminate                 │ │ │
│  │  │   - DoWhile (uses InMemoryWorkflowExecutor)                 │ │ │
│  │  │   - SetVariable (uses InMemoryExecutionDAOFacade)           │ │ │
│  │  │                                                             │ │ │
│  │  └────────────────────────────────────────────────────────────┘ │ │
│  │                              │                                   │ │
│  │                              ▼                                   │ │
│  │  ┌────────────────────────────────────────────────────────────┐ │ │
│  │  │                    Scheduling Loop                          │ │ │
│  │  │                                                             │ │ │
│  │  │   while (!workflow.isTerminal()) {                         │ │ │
│  │  │       outcome = deciderService.decide(workflow)            │ │ │
│  │  │       for (task : outcome.tasksToBeScheduled) {            │ │ │
│  │  │           if (isSystemTask(task))                          │ │ │
│  │  │               systemTaskExecutor.execute(task)             │ │ │
│  │  │           else                                             │ │ │
│  │  │               result = executeTaskActivity(task)  ───────┐ │ │ │
│  │  │               updateTaskState(task, result)              │ │ │ │
│  │  │       }                                                  │ │ │ │
│  │  │   }                                                      │ │ │ │
│  │  │   return workflow.output                                 │ │ │ │
│  │  │                                                          │ │ │ │
│  │  └──────────────────────────────────────────────────────────│─┘ │ │
│  │                                                              │   │ │
│  └──────────────────────────────────────────────────────────────│───┘ │
│                                                                  │     │
└──────────────────────────────────────────────────────────────────│─────┘
                                                                   │
                                                                   ▼
┌──────────────────────────────────────────────────────────────────────┐
│                         Temporal Activities                           │
│                                                                       │
│   executeSimpleTask(taskName, input) → output                        │
│   executeHttpTask(request) → response                                 │
│   executeSubWorkflow(workflowDef, input) → output                    │
│                                                                       │
└──────────────────────────────────────────────────────────────────────┘
```

## Task Type Mapping

| Conductor Task | Execution Strategy | Implementation |
|----------------|-------------------|----------------|
| `SIMPLE` | Temporal Activity | Worker task via activity |
| `HTTP` | Temporal Activity | HTTP activity |
| `SWITCH` / `DECISION` | System Task | Conductor's `Switch`/`Decision` class |
| `FORK_JOIN` | System Task | Conductor's `Fork` class |
| `JOIN` | System Task | Conductor's `Join` class |
| `TERMINATE` | System Task | Conductor's `Terminate` class + workflow status handling |
| `DO_WHILE` | System Task | Conductor's `DoWhile` class + in-memory adapters |
| `SET_VARIABLE` | System Task | Conductor's `SetVariable` class + in-memory adapters |
| `SUB_WORKFLOW` | Temporal Child Workflow | Child workflow execution |
| `WAIT` | Custom Handling | `Workflow.await()` with condition/timer |
| `INLINE` / `LAMBDA` | Local Activity | Inline execution |
| `FORK_JOIN_DYNAMIC` | Custom Handling | Dynamic parallel activities |

## In-Memory Adapters

To reuse Conductor's system task implementations within Temporal's deterministic execution model, we provide in-memory adapters that replace external dependencies with workflow-local state.

### InMemoryWorkflowExecutor

Implements `WorkflowExecutor` interface for system tasks that need to schedule additional tasks.

```
WorkflowExecutor (Conductor interface)
       │
       ▼
InMemoryWorkflowExecutor
       │
       ├── scheduleNextIteration()  ─── Used by DoWhile
       │         │
       │         └── Calls DeciderService.getTasksToBeScheduled()
       │             Adds tasks to WorkflowModel (Temporal makes this durable)
       │
       └── Other methods: no-op or UnsupportedOperationException
```

**Key method - `scheduleNextIteration()`:**
- Called by `DoWhile` to schedule the next loop iteration
- Uses `DeciderService.getTasksToBeScheduled()` to create task models
- Appends iteration number to task reference names (`task1` → `task1__2`)
- Adds tasks directly to `WorkflowModel.tasks` (no external persistence needed)
- Uses `Workflow.currentTimeMillis()` for deterministic timestamps

### InMemoryExecutionDAOFacade

Implements `ExecutionDAOFacade` interface as no-ops since Temporal handles persistence.

```
ExecutionDAOFacade (Conductor interface)
       │
       ▼
InMemoryExecutionDAOFacade
       │
       ├── updateWorkflow()  ─── Used by SetVariable
       │         │
       │         └── No-op: Temporal automatically persists workflow state
       │
       ├── removeTask()  ─── Used by DoWhile (keepLastN cleanup)
       │         │
       │         └── No-op: Temporal manages history size differently
       │
       └── Other methods: UnsupportedOperationException
```

## SystemTaskExecutor

The `SystemTaskExecutor` class delegates system task execution to Conductor's native implementations. This keeps Temporal code focused on orchestration while reusing Conductor's tested task logic.

### All System Tasks Use Conductor's Implementations

| Task | Conductor Class | Dependencies |
|------|-----------------|--------------|
| Fork | `Fork` | None |
| Join | `Join` | `ConductorProperties` |
| Switch | `Switch` | None |
| Decision | `Decision` | None |
| Terminate | `Terminate` | None |
| DoWhile | `DoWhile` | `ParametersUtils`, `InMemoryExecutionDAOFacade` |
| SetVariable | `SetVariable` | `ConductorProperties`, `ObjectMapper`, `InMemoryExecutionDAOFacade` |

### Workflow-Level Handling

The `TERMINATE` task requires additional handling in the workflow code (not in SystemTaskExecutor):

```kotlin
// After Terminate.execute() completes, workflow must update its status
if (task.taskType == TERMINATE && task.status == COMPLETED) {
    val terminationStatus = task.inputData["terminationStatus"]
    workflowModel.status = when (terminationStatus) {
        "COMPLETED" -> WorkflowModel.Status.COMPLETED
        "FAILED" -> WorkflowModel.Status.FAILED
        else -> WorkflowModel.Status.TERMINATED
    }
}
```

This is orchestration logic, not task execution logic - Conductor's `Terminate.execute()` only sets task status, not workflow status.

### Key Technical Finding

The `Join` task reads its `joinOn` list from `task.getInputData().get("joinOn")`, not from `workflowTask.joinOn`. The DeciderService handles this mapping when scheduling the task.

## Scheduling Loop

The core of the workflow is a scheduling loop that:

1. Calls `DeciderService.decide()` to determine ready tasks
2. Separates system tasks from worker tasks
3. Executes system tasks via `SystemTaskExecutor`
4. Executes worker tasks via Temporal activities
5. Re-evaluates async system tasks (like JOIN) after worker tasks complete
6. Repeats until workflow reaches terminal state

This mirrors how Conductor's `WorkflowExecutor` operates, but within Temporal's durable execution model.
