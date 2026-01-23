# Background and Prior Art

## Prior Art: temporal-airflow

The [temporal-airflow](../temporal-airflow/) project demonstrates executing Airflow DAGs on Temporal. Key pattern:

- Airflow models are ORM entities (SQLAlchemy)
- Solution: In-memory SQLite database per workflow execution
- Reuses Airflow's native `dag_run.update_state()` scheduling logic
- Activities execute individual task operators

## Conductor Architecture

Conductor separates concerns cleanly:

| Layer | Description |
|-------|-------------|
| **Models** | `WorkflowDef`, `WorkflowTask`, `WorkflowModel`, `TaskModel` - Plain POJOs |
| **DAOs** | `MetadataDAO`, `ExecutionDAO`, `QueueDAO`, etc. - Interface abstractions |
| **Execution** | `DeciderService`, `WorkflowExecutor` - Scheduling and orchestration logic |
| **Persistence** | SQLite, PostgreSQL, Redis implementations of DAOs |

**Key insight**: Unlike Airflow, Conductor's models are **not** ORM entities. They are plain Java objects serialized to/from the database by DAO implementations. This makes Conductor significantly easier to adapt.

## Conductor DAO Interfaces

Conductor defines 8 DAO interfaces:

| DAO | Purpose |
|-----|---------|
| `MetadataDAO` | WorkflowDef/TaskDef storage |
| `ExecutionDAO` | Workflow/Task execution state |
| `QueueDAO` | Task queues for workers |
| `IndexDAO` | Search/query workflows |
| `PollDataDAO` | Worker poll statistics |
| `RateLimitingDAO` | Task rate limiting |
| `ConcurrentExecutionLimitDAO` | Concurrency limits |
| `EventHandlerDAO` | Event handler configuration |

## Implementation Language

**Recommendation: Kotlin (JVM)**

| Factor | Rationale |
|--------|-----------|
| Conductor compatibility | Native access to Conductor Java libraries |
| Temporal SDK | Mature Java/Kotlin SDK |
| Expression evaluation | Reuse Conductor's GraalJS/Python evaluators |
| Type safety | Kotlin's null safety helps with complex Conductor models |

Alternative: TypeScript (if we reimplement expression evaluation)
