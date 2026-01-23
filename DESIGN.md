# Temporal Conductor: Design Document

## Overview

**Goal**: Execute [Netflix Conductor](https://conductor-oss.github.io/conductor/) workflow definitions using Temporal as the execution backend.

**Approach**: Reuse Conductor's core libraries (workflow models, expression evaluation, task mappers) with Temporal-backed DAO implementations.

## Design Documents

| Document | Description |
|----------|-------------|
| [01-background.md](design/01-background.md) | Prior art (temporal-airflow), Conductor architecture, language choice |
| [02-concurrency-model.md](design/02-concurrency-model.md) | Conductor's distributed locking model and implications |
| [03-in-memory-daos.md](design/03-in-memory-daos.md) | HashMap DAO design for in-workflow state |
| [04-architecture.md](design/04-architecture.md) | Core workflow architecture and task type mapping |
| [05-conductor-ui-integration.md](design/05-conductor-ui-integration.md) | Spring bean override for Conductor UI compatibility |
| [06-temporal-only-mode.md](design/06-temporal-only-mode.md) | Running Conductor with only Temporal (no Redis/PostgreSQL) |
| [07-poc-plan.md](design/07-poc-plan.md) | POC plan to validate critical assumptions before implementation |
| [08-timeout-mapping.md](design/08-timeout-mapping.md) | Conductor to Temporal timeout mapping and compatibility analysis |
| [09-poc3-plan.md](design/09-poc3-plan.md) | Detailed implementation plan for POC 3 (Temporal Workflow Integration) |

## Integration Modes

### Mode A: Hybrid (Conductor DB + Temporal Execution)

Use Temporal for workflow execution while keeping Conductor's database for UI and APIs.

- **Pros**: Full Conductor UI compatibility, gradual migration path
- **Cons**: Still requires Conductor's database infrastructure
- **Details**: [05-conductor-ui-integration.md](design/05-conductor-ui-integration.md)

### Mode B: Temporal-Only (No External Dependencies)

Replace all Conductor DAOs with Temporal-backed implementations.

- **Pros**: Single system, simplified operations, no Redis/PostgreSQL
- **Cons**: Limited search capabilities, UI needs adaptation
- **Details**: [06-temporal-only-mode.md](design/06-temporal-only-mode.md)

## Key Design Decisions

1. **HashMap DAOs for in-workflow state** - Temporal's single-threaded execution eliminates need for locks
2. **Reuse Conductor libraries** - DeciderService, expression evaluation, task mappers
3. **Implementation language: Kotlin** - Native JVM interop with Conductor
4. **Spring @Primary for UI integration** - Override beans without modifying Conductor

## Testing Strategy

- **Unit tests**: Test HashMap DAOs and scheduling logic in isolation
- **Integration tests**: Use Conductor's `conductor.db.type=memory` mode
- **End-to-end tests**: Run sample Conductor workflows through Temporal

## Open Questions

1. **SIMPLE task execution**: Call existing Conductor workers via HTTP, or define new Temporal activities?
2. **TaskDef source**: Load from Conductor server API, or bundle with workflow definition?
3. **Workflow registration**: Accept raw JSON, or require pre-registration?

## Next Steps

**Current Phase: POC Validation**

Before implementation, 5 critical assumptions must be validated. See [07-poc-plan.md](design/07-poc-plan.md) for details.

| POC | Goal | Est. Effort |
|-----|------|-------------|
| 1 | DeciderService runs standalone | 2-3 days |
| 2 | Conductor code is deterministic | 3-4 days |
| 3 | Full Temporal workflow integration | 4-5 days |
| 4 | Spring @Primary override works | 1-2 days |
| 5 | State fits Temporal limits | 2 days |

**After POC Success:**
1. Set up Kotlin/Gradle project with Conductor dependencies
2. Implement `InMemoryMetadataDAO` and `InMemoryExecutionDAO`
3. Wire up `DeciderService` with in-memory DAOs
4. Implement basic scheduling loop in Temporal workflow
5. Add activities for SIMPLE and HTTP tasks
6. Test with sample Conductor workflow definitions

## References

- Conductor source: `./conductor/` (git worktree)
- temporal-airflow: `../temporal-airflow/`
- Conductor OSS docs: https://conductor-oss.github.io/conductor/
- Temporal Java SDK: https://docs.temporal.io/develop/java
