# Design Decision: In-Memory HashMap DAOs

## Conductor's Built-in In-Memory Mode

Conductor already provides an in-memory persistence option:

```properties
# server/src/main/resources/application.properties (default!)
conductor.db.type=memory
```

This uses `JedisMock` (wrapping `rarefiedredis.RedisMock`) to provide in-memory storage for all Redis DAOs. It's used for integration tests and development.

**Why we're not using it:**

| Concern | Issue |
|---------|-------|
| **Spring Boot required** | Full Spring context needed, awkward inside Temporal workflow |
| **Dependency weight** | Pulls in jedis, rarefiedredis, orkes-conductor-queues |
| **Unnecessary components** | Includes QueueDAO, IndexDAO, PollDataDAO we don't need |
| **Determinism risk** | Spring auto-wiring/lazy init could cause replay issues |

We may use Conductor's in-memory mode for **integration testing** outside the workflow, but for the workflow itself we'll implement minimal HashMap DAOs.

## Rationale

A simple `HashMap`-based implementation satisfies all requirements because:

1. **Single-threaded execution** - Temporal workflow code runs single-threaded by design
2. **No concurrent modifications** - Only one "thread" accesses the in-memory state
3. **Durability handled by Temporal** - Event history provides persistence and replay
4. **Minimal scope** - We only manage ONE workflow's state per execution

## What We Reuse from Conductor

| Component | Purpose | Notes |
|-----------|---------|-------|
| `conductor-common` | WorkflowDef, WorkflowTask, TaskType, TaskDef models | Direct dependency |
| `ParametersUtils` | Expression evaluation (`${workflow.input.x}`) | Copy/adapt |
| `Evaluators` | JavaScript/Python expression support | Copy/adapt |
| `TaskMappers` | SWITCH, FORK_JOIN, DO_WHILE logic | Via DeciderService |
| `DeciderService` | Core scheduling: "what tasks are ready?" | Direct use |

## What We Replace

| Component | Conductor | Our Implementation |
|-----------|-----------|-------------------|
| `MetadataDAO` | Database-backed | HashMap of TaskDef/WorkflowDef |
| `ExecutionDAO` | Database-backed | HashMap of WorkflowModel/TaskModel |
| `QueueDAO` | Redis/DB task queues | Not needed (Temporal handles) |
| `IndexDAO` | Elasticsearch/DB indexing | Not needed (Temporal handles) |
| `ExecutionLockService` | Redis/Zookeeper locks | Not needed (single-threaded) |

## DAO Methods Required

**MetadataDAO** (minimal):
- `getTaskDef(String name)` - Lookup task definitions

**ExecutionDAO** (minimal):
- `createWorkflow(WorkflowModel)` / `updateWorkflow(WorkflowModel)`
- `getWorkflow(String workflowId, boolean includeTasks)`
- `createTasks(List<TaskModel>)` / `updateTask(TaskModel)`
- `getTasksForWorkflow(String workflowId)`

All other methods can throw `UnsupportedOperationException` or return empty results.
