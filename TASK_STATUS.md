# Task Status: Temporal Conductor

## Task Overview
- **Task**: Execute Conductor workflows using Temporal
- **Created**: 2026-01-17
- **Workspace**: projects
- **Reference**: Similar integration exists for Airflow at `temporal-airflow`
- **Design Documents**: [DESIGN.md](./DESIGN.md) (index) and [design/](./design/) (detailed docs)

## Goal
Build a system that can execute [Netflix Conductor](https://conductor-oss.github.io/conductor/) workflows using Temporal as the execution backend, similar to how `temporal-airflow` executes Airflow DAGs.

## Research Findings

### Conductor Workflow Model

**WorkflowDef** (`conductor/common/src/main/java/.../WorkflowDef.java`):
- `name`: Workflow identifier
- `version`: Workflow version number
- `tasks`: List of WorkflowTask (the workflow steps)
- `inputParameters`: List of expected input parameter names
- `outputParameters`: Map of output expressions
- `failureWorkflow`: Workflow to run on failure
- `timeoutSeconds`: Workflow timeout
- `variables`: Global workflow variables

**WorkflowTask** (`conductor/common/src/main/java/.../WorkflowTask.java`):
- `name`: Task name
- `taskReferenceName`: Unique reference within workflow
- `type`: Task type (SIMPLE, SWITCH, FORK_JOIN, etc.)
- `inputParameters`: Map with expression support (`${workflow.input.x}`, `${taskRef.output.y}`)
- `decisionCases`: For SWITCH/DECISION - map of case value to task lists
- `defaultCase`: Default branch for SWITCH/DECISION
- `forkTasks`: For FORK_JOIN - list of parallel task lists
- `loopOver`: For DO_WHILE - tasks to loop
- `loopCondition`: Expression for DO_WHILE continuation
- `subWorkflowParam`: For SUB_WORKFLOW - nested workflow reference
- `joinOn`: For JOIN - task refs to wait for

### Conductor Task Types (from TaskType.java)

| Type | Description | Temporal Mapping |
|------|-------------|------------------|
| `SIMPLE` | Worker-executed task | Activity |
| `DYNAMIC` | Dynamic task name from input | Activity with dynamic name |
| `SWITCH` / `DECISION` | Conditional branching | if/match in workflow |
| `FORK_JOIN` | Static parallel execution | Promise.all() / asyncio.gather() |
| `FORK_JOIN_DYNAMIC` | Dynamic parallel execution | Dynamic Promise.all() |
| `JOIN` | Wait for parallel branches | Implicit in fork completion |
| `DO_WHILE` | Loop with condition | while loop in workflow |
| `SUB_WORKFLOW` | Child workflow execution | Child workflow |
| `START_WORKFLOW` | Async workflow start | Signal/start workflow |
| `HTTP` | HTTP request | HTTP activity |
| `WAIT` | Wait for signal/duration | workflow.wait_condition/sleep |
| `EVENT` | Publish/wait events | Signals |
| `HUMAN` | Human task | External signal wait |
| `LAMBDA` / `INLINE` | Inline code execution | Local activity or inline |
| `TERMINATE` | End workflow | Return/raise |
| `SET_VARIABLE` | Set workflow variable | State assignment |
| `NOOP` | No operation | Pass |

### Input Parameter Expressions

Conductor uses JSONPath-like expressions:
- `${workflow.input.paramName}` - Workflow input
- `${taskRefName.output.field}` - Task output
- `${workflow.variables.varName}` - Workflow variables
- JavaScript expressions in `evaluatorType: "javascript"`

### temporal-airflow Pattern Analysis

The `temporal-airflow` integration uses an **"executor pattern"**:

1. **Workflow receives DAG definition** (serialized)
2. **In-workflow scheduling loop** uses Airflow's native `update_state()` logic
3. **Activities execute individual tasks** - load DAG, run operator, return result
4. **XCom stored in workflow state** for cross-task data passing
5. **Batched sync to external DB** for UI visibility

Key files:
- `deep_workflow.py`: Main workflow with scheduling loop
- `models.py`: Pydantic models for input/output
- `activities.py`: Task execution activity

## Architecture Options

### Option A: Interpreter Pattern (Recommended)
Similar to temporal-airflow: a single workflow interprets the Conductor definition.

```
ConductorWorkflow:
  1. Parse WorkflowDef JSON
  2. Build dependency graph
  3. Scheduling loop:
     - Find ready tasks (deps complete)
     - Execute via activities
     - Handle control flow (SWITCH, FORK, DO_WHILE)
     - Store outputs for expressions
  4. Return final output
```

**Pros**: Single workflow type, easier debugging, handles all task types
**Cons**: Complex workflow logic, expressions evaluated in workflow

### Option B: Code Generation
Generate Temporal workflow code from Conductor definition.

**Pros**: Native Temporal constructs, potentially better performance
**Cons**: Requires generation step, harder to update

### Option C: Hybrid
Use interpreter for control flow, generate activities for SIMPLE tasks.

## Implementation Language Choice

| Language | Pros | Cons |
|----------|------|------|
| **Python** | Expression eval easy, pydantic models | GIL, async complexity |
| **TypeScript** | Good async, JSON native | Expression eval needs work |
| **Java/Kotlin** | Native to Conductor, type safety | More verbose |
| **Go** | Performance, native Temporal support | Expression eval harder |

**Recommendation**: Python or TypeScript for initial implementation (expression evaluation is critical)

## Current Status

### ✅ POC COMPLETE - All Phases Implemented

**Total Tests:** 90 passing
**Last Verified:** 2026-01-21
**Temporal Integration:** Verified against real Temporal dev server

---

### Phase 1: Research & Design (Complete)
- [x] Research Conductor workflow model
- [x] Analyze temporal-airflow integration pattern
- [x] Investigate Conductor concurrency model (no transactions, distributed locking)
- [x] Decision: Use HashMap-based in-memory DAOs
- [x] Create design document
- [x] Design Conductor UI integration (Spring @Primary bean override)
- [x] Evaluate Temporal-only mode (no Redis/PostgreSQL)
- [x] Split design docs into modular files
- [x] Comprehensive risk evaluation
- [x] **POC plan created** → [design/07-poc-plan.md](design/07-poc-plan.md)

### Phase 2: POC Validation (Complete)

| POC | Description | Status | Blocking? |
|-----|-------------|--------|-----------|
| 1 | DeciderService Isolation | ✅ **PASSED** | Yes |
| 2 | Determinism Audit | ✅ **PASSED** | Yes |
| 3 | Temporal Workflow Integration | ✅ **PASSED** | Yes |
| 4 | Spring @Primary Override | ✅ **PASSED** (via profiles) | Yes |
| 5 | State Size Analysis | ⬜ Deferred | No |

#### POC 1 Results (DeciderService Isolation)
**Status:** ✅ PASSED
**Location:** `poc-1-decider-isolation/`

Key findings:
- DeciderService can be instantiated manually with HashMap-based DAOs
- Simple workflow scheduling works (t1 → t2 → complete)
- Expression evaluation `${workflow.input.x}` works via ParametersUtils
- DeciderOutcome fields are package-private - need accessor class in same package
- Requires Java 21 (conductor-core 3.21.23 dependency)

#### POC 2 Results (Determinism Audit)
**Status:** ✅ PASSED (with mitigation)
**Location:** `poc-2-determinism-audit/`

Findings using ByteBuddy instrumentation + code review:
- **BLOCKING:** `IDGenerator.generate()` uses `UUID.randomUUID()`
  - Called in `DeciderService.getTasksToBeScheduled()` line 882
  - Generates task IDs during workflow scheduling
- **BLOCKING:** `System.currentTimeMillis()` used in timeout checks
- **0 random() calls** in core scheduling path

**Mitigations implemented:** `conductor-temporal-ext/`
- `IdGeneratorProvider` interface for pluggable ID generation
- `DeterministicIdGenerator` produces `{prefix}-{sequence}` format IDs
- `TemporalDeciderServiceFactory` accepts custom ID generator
- **Timeout strategy:** Disable Conductor timeouts (set to 0), use Temporal timeouts instead

#### POC 3 Results (Temporal Workflow Integration)
**Status:** ✅ PASSED
**Location:** `poc-3-temporal-integration/` and `conductor-server-temporal/`

Key achievements:
- Single Temporal workflow (`ConductorWorkflowImpl`) interprets Conductor WorkflowDef at runtime
- All critical task types working: SIMPLE, HTTP, FORK_JOIN, JOIN, SWITCH, DO_WHILE, SET_VARIABLE, TERMINATE
- Custom search attributes for Conductor visibility (ConductorWorkflowType, ConductorStatus, etc.)
- Deterministic ID generation and timestamp handling for replay safety

#### POC 4 Results (Spring @Primary Override)
**Status:** ✅ PASSED (implemented via Spring Profiles)
**Location:** `conductor-server-temporal/`

Implementation approach:
- `@Profile("stub")` - StubWorkflowService, StubTaskService, StubEventService (for testing)
- `@Profile("temporal")` - TemporalWorkflowService, TemporalTaskService, TemporalEventService
- Profile-based switching eliminates need for @Primary bean conflicts

### Phase 3: Implementation (Complete)

#### Workflow Observability (Phase 1 Design)
- [x] ConductorWorkflowImpl with query methods
- [x] Custom search attributes registered and working
- [x] `getWorkflow()` query returns current workflow state
- [x] Fix: Use `lastUpdateTime` field for query safety (avoid Workflow.currentTimeMillis() in queries)

#### REST API Layer (Phase 2 Design)
- [x] WorkflowResource - CRUD operations for workflows
- [x] TaskResource - Task operations with UpdateTaskRequest DTO
- [x] MetadataResource - Workflow/Task definition management
- [x] EventResource - Workflow lifecycle events
- [x] Swagger/OpenAPI integration verified

#### Temporal Backend Integration
- [x] TemporalConfig - Spring configuration for WorkflowClient, WorkerFactory
- [x] TemporalWorkflowService - Real workflow service using Temporal
- [x] TemporalMetadataService - In-memory metadata storage (ConcurrentHashMap)
- [x] TemporalTaskService - Task operations via Temporal queries and signals

### Test Coverage

| Test Class | Tests | Package |
|-----------|-------|---------|
| EventResourceTest | 6 | io.temporal.conductor.api |
| MetadataResourceTest | 10 | io.temporal.conductor.api |
| OpenApiIntegrationTest | 9 | io.temporal.conductor.api |
| TaskResourceTest | 6 | io.temporal.conductor.api |
| WorkflowResourceTest | 11 | io.temporal.conductor.api |
| TemporalMetadataServiceTest | 19 | io.temporal.conductor.service.temporal |
| TemporalTaskServiceTest | 12 | io.temporal.conductor.service.temporal |
| TemporalWorkflowServiceTest | 10 | io.temporal.conductor.service.temporal |
| ConductorWorkflowTest | 7 | io.temporal.conductor.workflow |
| **Total** | **90** | |

### Real Temporal Server Verification

Successfully tested against Temporal dev server (2026-01-21):
1. Started Temporal dev server with `temporal server start-dev`
2. Created `conductor` namespace with custom search attributes
3. Started Spring Boot with `temporal` profile
4. Executed workflows - verified in Temporal UI with search attributes
5. All REST API endpoints functional

---

### Issues Resolved

1. **Jackson version conflict** - Removed explicit jackson-databind:2.17.0, let Spring Boot manage versions
2. **Search attributes not defined** - Added TestEnvironmentOptions.registerSearchAttribute() in tests
3. **Workflow.currentTimeMillis() in query** - Added lastUpdateTime field for query safety
4. **HTTP 415 on task update** - Created UpdateTaskRequest DTO wrapper for proper deserialization
5. **Namespace not found** - Created conductor namespace with temporal CLI

## Key Design Decisions

1. **In-memory HashMap DAOs** - No need for SQLite or real database
   - Conductor uses distributed locking, not transactions
   - Temporal provides single-threaded execution guarantee
   - Temporal event history provides durability

2. **Reuse Conductor libraries** - Direct dependency on `conductor-common` and `conductor-core`
   - Expression evaluation (ParametersUtils, Evaluators)
   - Task mappers (SWITCH, FORK_JOIN, DO_WHILE)
   - DeciderService scheduling logic

3. **Implementation language: Kotlin** - Native JVM interop with Conductor

4. **Conductor UI Integration via Spring @Primary** - No Conductor modifications required
   - Create custom module with `@Primary @Service TemporalWorkflowService`
   - Overrides `WorkflowServiceImpl` without touching Conductor codebase
   - Conductor uses `@ComponentScan` (no `@ConditionalOnMissingBean`)
   - Deploy as custom Conductor release including our module
   - Sync activities write state back to Conductor DB for UI visibility

5. **Temporal-Only Mode is feasible** - See [design/06-temporal-only-mode.md](design/06-temporal-only-mode.md)
   - All 8 Conductor DAOs can be backed by Temporal
   - Core execution, task queuing, rate limiting: native Temporal features
   - Search/indexing: limited to Temporal search attributes (no full-text)
   - Trade-off: simpler ops vs reduced search capabilities

## Resolved Decisions

1. **SIMPLE task execution**: ✅ Temporal activities (`TaskExecutionActivitiesImpl`)
   - Worker tasks execute via Temporal activity abstraction
2. **TaskDef source**: ✅ In-memory metadata service
   - TaskDefs stored in ConcurrentHashMap via MetadataService
3. **Workflow registration**: ✅ REST API + in-memory storage
   - Definitions stored via MetadataResource, no external DB required
4. **Target Conductor version**: ✅ conductor-common 3.21.23
   - Using Conductor libraries as dependencies, not full Conductor deployment
5. **Priority task types**: ✅ All implemented
   - Working: SIMPLE, HTTP, SWITCH, FORK_JOIN, JOIN, DO_WHILE, SET_VARIABLE, TERMINATE
   - Not yet tested: SUB_WORKFLOW, WAIT

## Remaining Work (Future Phases)

1. **SUB_WORKFLOW support** - Child workflow execution
2. **WAIT task type** - Duration/signal-based waiting
3. **Production deployment** - Docker/K8s configuration
4. **Conductor UI integration** - Connect existing Conductor UI to this backend
5. **State size optimization** - Large workflow payload handling (POC 5)

## References

- Conductor OSS: https://conductor-oss.github.io/conductor/
- Conductor source: `./conductor/` (git worktree)
- temporal-airflow: `../temporal-airflow/`
- Design document: `./DESIGN.md`
