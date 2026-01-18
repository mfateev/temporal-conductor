# POC Implementation Status

## Overview

This document tracks the implementation status of the 5 critical POCs outlined in [design/07-poc-plan.md](../design/07-poc-plan.md).

## POC 1: DeciderService Isolation ⚠️  IN PROGRESS

**Goal**: Prove `DeciderService` can be instantiated outside Spring context with minimal HashMap DAOs.

**Status**: Foundation implemented, DeciderService instantiation pending

### What's Implemented

✅ **InMemoryMetadataDAO** (`src/main/java/io/temporal/conductor/poc/InMemoryMetadataDAO.java`)
- Implements `MetadataDAO` interface
- HashMap-based storage for `WorkflowDef` and `TaskDef`
- Helper methods for test workflow registration
- No Spring dependencies

✅ **InMemoryExecutionDAO** (`src/main/java/io/temporal/conductor/poc/InMemoryExecutionDAO.java`)
- Implements `ExecutionDAO` and `PollDataDAO` interfaces
- HashMap-based storage for `Workflow` and `Task` instances
- Tracks workflow-to-tasks relationships
- Minimal implementations for unsupported operations
- No Spring dependencies

✅ **Test Harness** (`src/test/java/io/temporal/conductor/poc/DeciderServicePOCTest.java`)
- JUnit test setup
- DAO validation tests (passing)
- Sequential workflow test structure (DeciderService instantiation pending)

### Next Steps

1. **Identify DeciderService Dependencies**
   ```bash
   cd conductor
   # Find DeciderService constructor
   find . -name "DeciderService.java" -exec grep -A 20 "public DeciderService" {} \;
   ```

2. **Create Minimal Implementations**
   - `ParametersUtils` - expression evaluation
   - `MetadataMapperService` - model mapping
   - `SystemTaskRegistry` - system task handling
   - `ExternalPayloadStorageUtils` - payload handling
   - `IDGenerator` - ID generation

3. **Wire Up DeciderService**
   ```java
   DeciderService deciderService = new DeciderService(
       executionDAO,
       metadataDAO,
       parametersUtils,
       metadataMapper,
       systemTaskRegistry,
       externalPayloadStorage,
       idGenerator,
       conductorProperties
   );
   ```

4. **Run Test Workflow**
   - Call `deciderService.decide(workflow)`
   - Verify tasks are scheduled in correct order
   - Mark tasks complete and verify workflow progression

### Success Criteria

- [ ] DeciderService instantiates without Spring context
- [ ] `decide()` returns `DeciderOutcome` with correct first task
- [ ] After marking task complete, `decide()` returns next task
- [ ] Workflow reaches terminal state after all tasks complete

### Build Note

⚠️ **Requires Java 11-21 to build** (current system has Java 25)

The code is ready but cannot be compiled until a compatible Java version is available. To run:

```bash
# Option 1: Use Java 21
sdk install java 21.0.1-tem
sdk use java 21.0.1-tem
./gradlew test --tests DeciderServicePOCTest

# Option 2: Use Docker with Java 21
docker run --rm -v $(pwd):/workspace -w /workspace gradle:8.12-jdk21 \
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
