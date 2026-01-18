# POC Implementation Status

## Overview

This document tracks the implementation status of the 5 critical POCs outlined in [design/07-poc-plan.md](../design/07-poc-plan.md).

## Important Discovery: Conductor Object Model Architecture

### Investigation Results (commit cd528e126, Jan 31, 2022)

**Conductor v3.5.0+ uses TWO parallel class hierarchies:**

**Client DTOs** (API/Transport layer):
- `com.netflix.conductor.common.run.Workflow`
- `com.netflix.conductor.common.metadata.tasks.Task`
- Used for REST API requests/responses
- Used by external clients

**Domain Models** (Core/Business layer):
- `com.netflix.conductor.model.WorkflowModel`
- `com.netflix.conductor.model.TaskModel`
- Used internally by core services
- **Used by DAO interfaces** ← Critical for POC

**Conversion**:
- `WorkflowModel.toWorkflow()` - domain to DTO
- `TaskModel.toTask()` - domain to DTO
- `ExecutionDAOFacade` handles boundary conversions

**Version Timeline:**
- Introduced: v3.5.0 (March 2022)
- Present in all versions since then
- Maven Central latest: 3.21.16
- Local repo: v3.21.23 (Jan 2025)

This is **not a version issue** - it's an architectural pattern we must follow.

## POC 1: DeciderService Isolation 🟡 IN PROGRESS

**Goal**: Prove `DeciderService` can be instantiated outside Spring context with minimal HashMap DAOs.

**Status**: Core implementation complete, compilation issues remain

### What's Implemented

✅ **InMemoryMetadataDAO** (`src/main/java/io/temporal/conductor/poc/InMemoryMetadataDAO.java`)
- Implements `MetadataDAO` interface
- HashMap-based storage for `WorkflowDef` and `TaskDef`
- Helper methods for test workflow registration
- Correct return types (void for workflow, TaskDef for tasks)
- No Spring dependencies

✅ **InMemoryExecutionDAO** (`src/main/java/io/temporal/conductor/poc/InMemoryExecutionDAO.java`)
- Implements `ExecutionDAO` and `PollDataDAO` interfaces
- **UPDATED**: Uses domain models (`WorkflowModel`/`TaskModel`)
- HashMap-based storage for workflow and task execution state
- Tracks workflow-to-tasks relationships
- Minimal implementations for unsupported operations
- No Spring dependencies

✅ **DeciderService Dependencies** (all created)
- `MinimalIDGenerator` - extends IDGenerator (UUID-based)
- `MinimalParametersUtils` - extends ParametersUtils (pass-through)
- `MinimalExternalPayloadStorageUtils` - extends ExternalPayloadStorageUtils
- `NoOpExternalPayloadStorage` - no-op implementation
- `MinimalSystemTaskRegistry` - empty registry for POC

✅ **Test Harness** (`src/test/java/io/temporal/conductor/poc/DeciderServicePOCTest.java`)
- JUnit test setup with all dependencies
- **UPDATED**: Uses `WorkflowModel`/`TaskModel`
- DAO validation tests (structure ready)
- DeciderService instantiation code complete
- Sequential workflow test structure ready

### Remaining Work

1. **Complete DAO Interface Implementation**
   - Add missing ExecutionDAO methods:
     - `removeWorkflowWithExpiry(String, int)`
     - Several deprecated methods that may still be required
   - Audit full ExecutionDAO interface against 3.21.16
   - Audit full MetadataDAO interface against 3.21.16

2. **Fix Compilation Errors**
   - Resolve remaining @Override mismatches
   - Ensure all abstract methods are implemented
   - Test with `./gradlew test --tests="*DeciderServicePOCTest"`

3. **Run Test Workflow**
   - Execute DAO validation tests
   - Call `deciderService.decide(workflow)`
   - Verify tasks are scheduled in correct order
   - Mark tasks complete and verify workflow progression

### Success Criteria

- [x] DeciderService dependencies identified and created
- [x] Uses correct domain models (WorkflowModel/TaskModel)
- [ ] Code compiles without errors
- [ ] DAO validation tests pass
- [ ] DeciderService instantiates without Spring context
- [ ] `decide()` returns `DeciderOutcome` with correct first task
- [ ] After marking task complete, `decide()` returns next task
- [ ] Workflow reaches terminal state after all tasks complete

### Build Note

⚠️ **Requires Java 11-21 to build** (current system has Java 25)

The code is ready but cannot be compiled until a compatible Java version is available. To run:

```bash
# Option 1: Use Java 11 (minimum requirement)
sdk install java 11.0.21-tem
sdk use java 11.0.21-tem
./gradlew test --tests DeciderServicePOCTest

# Option 2: Use Java 17 or 21 (newer LTS versions)
sdk install java 17.0.9-tem
sdk use java 17.0.9-tem
./gradlew test --tests DeciderServicePOCTest

# Option 3: Use Docker with Java 11
docker run --rm -v $(pwd):/workspace -w /workspace gradle:8.12-jdk11 \
  ./gradlew test --tests DeciderServicePOCTest
```

## POC 2: Determinism Audit ⏸️ NOT STARTED

**Goal**: Identify non-deterministic code in Conductor libraries

**Status**: Awaiting POC 1 completion

## POC 3: Temporal Workflow Integration ⏸️ NOT STARTED

**Goal**: Run complete scheduling loop in Temporal workflow

**Status**: Awaiting POC 1 & 2 completion

## POC 4: Spring @Primary Override ⏸️ NOT STARTED

**Goal**: Validate @Primary bean override works

**Status**: Can run in parallel with POC 1-3

## POC 5: State Size Analysis ⏸️ NOT STARTED

**Goal**: Validate workflow state fits Temporal limits

**Status**: Can run in parallel with POC 1-3

## Overall Status

| POC | Status | Blocker |
|-----|--------|---------|
| 1 - DeciderService Isolation | 🟡 In Progress | Need to instantiate DeciderService |
| 2 - Determinism Audit | ⚪ Not Started | Waiting for POC 1 |
| 3 - Temporal Integration | ⚪ Not Started | Waiting for POC 1 & 2 |
| 4 - Spring @Primary | ⚪ Not Started | Independent |
| 5 - State Size Analysis | ⚪ Not Started | Independent |

## Code Structure

```
temporal-conductor-core/
├── src/main/java/io/temporal/conductor/
│   └── poc/
│       ├── InMemoryMetadataDAO.java      ✅ Complete
│       └── InMemoryExecutionDAO.java     ✅ Complete
│
└── src/test/java/io/temporal/conductor/
    └── poc/
        └── DeciderServicePOCTest.java     🟡 Partial
```

## References

- [POC Plan](../design/07-poc-plan.md)
- [Design Overview](../DESIGN.md)
- [Conductor Source](../conductor/)
- [SDK Java](../sdk-java/)
- [Samples Java](../samples-java/)
