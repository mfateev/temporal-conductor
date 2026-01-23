# POC Plan: Validating Deep Integration Approach

## Overview

Before full implementation, we must validate 5 critical assumptions that could require architectural redesign if proven false.

## Critical Risks Requiring Validation

| # | Risk | Impact if Wrong |
|---|------|-----------------|
| 1 | DeciderService can run standalone without Spring | Complete redesign required |
| 2 | Conductor code is deterministic for Temporal replay | Workflow replay failures |
| 3 | Expression evaluation (GraalJS) is safe in workflows | Non-determinism errors |
| 4 | Spring @Primary bean override works | Must fork Conductor |
| 5 | Workflow state fits Temporal limits | Need external storage |

---

## POC 1: DeciderService Isolation

**Priority**: Critical (blocker)

**Goal**: Prove `DeciderService` can be instantiated and called outside Spring context with minimal HashMap DAOs.

**Steps**:
1. Create Kotlin project with `conductor-core` dependency
2. Implement minimal `InMemoryMetadataDAO`:
   - `getTaskDef(name: String): TaskDef?`
3. Implement minimal `InMemoryExecutionDAO`:
   - `createWorkflow(workflow: WorkflowModel)`
   - `updateWorkflow(workflow: WorkflowModel)`
   - `getWorkflow(workflowId: String, includeTasks: Boolean): WorkflowModel?`
   - `createTasks(tasks: List<TaskModel>)`
   - `updateTask(task: TaskModel)`
4. Identify all `DeciderService` constructor dependencies
5. Instantiate `DeciderService` manually (no Spring)
6. Call `decide(workflow)` with a simple 3-task workflow
7. Verify `DeciderOutcome` contains correct tasks to schedule

**Test Workflow**:
```json
{
  "name": "simple-test",
  "tasks": [
    { "name": "task1", "taskReferenceName": "t1", "type": "SIMPLE" },
    { "name": "task2", "taskReferenceName": "t2", "type": "SIMPLE" }
  ]
}
```

**Success Criteria**:
- [ ] `DeciderService` instantiates without Spring context
- [ ] `decide()` returns `DeciderOutcome` with `t1` as first task to schedule
- [ ] After marking `t1` complete, `decide()` returns `t2`
- [ ] After marking `t2` complete, workflow reaches terminal state

**Failure Mitigation**: If DeciderService has too many dependencies, evaluate:
- Extracting core scheduling logic into standalone class
- Using Spring context in activities only (not workflow code)

**Estimated Effort**: 2-3 days

---

## POC 2: Determinism Audit

**Priority**: Critical

**Goal**: Identify all non-deterministic code paths in Conductor libraries that would break Temporal workflow replay.

**Non-Deterministic Patterns to Detect**:
- `System.currentTimeMillis()` / `System.nanoTime()`
- `new Date()` / `Instant.now()` / `LocalDateTime.now()`
- `UUID.randomUUID()`
- `Math.random()` / `Random` / `SecureRandom`
- `Thread.currentThread()` state access
- Network/file I/O

**Steps**:
1. Use Java instrumentation (ByteBuddy or AspectJ) to intercept:
   ```kotlin
   // Intercept these classes/methods
   System.currentTimeMillis()
   System.nanoTime()
   UUID.randomUUID()
   Random.*
   Date.<init>()
   Instant.now()
   ```
2. Run `DeciderService.decide()` with test workflows
3. Log all intercepted calls with stack traces
4. Categorize findings:
   - **Blocking**: Used in scheduling decisions
   - **Cosmetic**: Used only for logging/debugging
   - **Containable**: Can be mocked/wrapped

**Key Areas to Audit**:
- `ParametersUtils` - expression evaluation
- `TaskMappers` - SWITCH, FORK_JOIN, DO_WHILE logic
- `DeciderService.decide()` - core scheduling
- GraalJS `ScriptEngine` - JavaScript evaluation

**Success Criteria**:
- [ ] Complete list of non-deterministic calls
- [ ] Each call categorized (blocking/cosmetic/containable)
- [ ] Mitigation strategy for each blocking call

**Failure Mitigation**: If blocking non-determinism found:
- Wrap affected code in Temporal activities (not workflow)
- Fork and patch Conductor classes
- Use `Workflow.sideEffect()` for unavoidable non-determinism

**Estimated Effort**: 3-4 days

---

## POC 3: Temporal Workflow Integration

**Priority**: Critical (depends on POC 1 & 2)

**Goal**: Run the complete scheduling loop inside a real Temporal workflow.

**Steps**:
1. Set up Temporal Java SDK project
2. Create `ConductorWorkflow` implementation:
   ```kotlin
   @WorkflowInterface
   interface ConductorWorkflow {
       @WorkflowMethod
       fun execute(workflowDef: WorkflowDef, input: Map<String, Any>): Map<String, Any>
   }
   ```
3. Implement in-memory DAOs as workflow state (must be serializable)
4. Implement scheduling loop:
   ```kotlin
   while (!workflow.isTerminal) {
       val outcome = deciderService.decide(workflow)
       for (task in outcome.tasksToBeScheduled) {
           val result = activities.executeTask(task)
           updateTaskState(task, result)
       }
   }
   ```
5. Create `TaskExecutionActivities`:
   ```kotlin
   @ActivityInterface
   interface TaskExecutionActivities {
       fun executeSimpleTask(taskName: String, input: Map<String, Any>): Map<String, Any>
   }
   ```
6. Test with progressively complex workflows:
   - 2 sequential SIMPLE tasks
   - SIMPLE + SWITCH + SIMPLE
   - FORK_JOIN with 2 parallel branches
   - DO_WHILE loop

**Success Criteria**:
- [ ] Simple sequential workflow completes
- [ ] SWITCH branches correctly based on condition
- [ ] FORK_JOIN executes branches in parallel
- [ ] DO_WHILE iterates correct number of times
- [ ] Workflow survives worker restart (replay works)

**Failure Mitigation**: If replay fails:
- Move non-deterministic code to activities
- Use `Workflow.sideEffect()` for ID generation
- Use `Workflow.currentTimeMillis()` for timestamps

**Estimated Effort**: 4-5 days

---

## POC 4: Spring @Primary Bean Override

**Priority**: High

**Goal**: Validate that our `@Primary` service overrides Conductor's `WorkflowServiceImpl`.

**Steps**:
1. Create Spring Boot application with dependencies:
   ```kotlin
   implementation("io.orkes.conductor:orkes-conductor-server:3.x")
   implementation("io.temporal:temporal-sdk:1.x")
   ```
2. Create override service:
   ```kotlin
   @Service
   @Primary
   class TemporalWorkflowService : WorkflowService {
       override fun startWorkflow(request: StartWorkflowRequest): String {
           logger.info(">>> Routing to Temporal!")
           // Start Temporal workflow
           return workflowId
       }
       // ... other methods delegate to original or Temporal
   }
   ```
3. Add our package to component scan
4. Start Conductor server
5. Call `POST /api/workflow` via REST
6. Verify logs show our service handling request

**Verification Points**:
- [ ] Our `TemporalWorkflowService` bean is created
- [ ] It takes precedence over `WorkflowServiceImpl`
- [ ] REST controllers receive our bean (not original)
- [ ] Other Conductor services still function

**Failure Mitigation**: If @Primary doesn't work:
- Use `@Qualifier` with custom `BeanPostProcessor`
- Fork Conductor to add pluggable `WorkflowService` interface
- Use AspectJ to intercept `WorkflowServiceImpl` methods

**Estimated Effort**: 1-2 days

---

## POC 5: State Size Analysis

**Priority**: High

**Goal**: Validate that workflow state fits within Temporal's payload limits.

**Temporal Limits**:
- Single payload: 2MB (warn), 4MB (error)
- Total workflow state: Larger but best kept reasonable

**Steps**:
1. Collect 5+ real-world complex Conductor workflows:
   - High task count (50+ tasks)
   - Large payloads (1KB+ per task input/output)
   - Deep nesting (sub-workflows, loops)
2. Simulate execution, tracking state at each step:
   ```kotlin
   data class WorkflowState(
       val workflow: WorkflowModel,      // Core workflow state
       val tasks: Map<String, TaskModel>, // All task states
       val variables: Map<String, Any>    // Workflow variables
   )
   ```
3. Serialize state to JSON at each decision point
4. Measure:
   - Peak state size
   - State growth rate per task
   - Largest individual components
5. Compare against Temporal limits

**Analysis Template**:
| Workflow | Tasks | Peak State Size | Largest Component |
|----------|-------|-----------------|-------------------|
| order-processing | 12 | 45KB | task outputs |
| data-pipeline | 85 | 2.1MB | task inputs |
| etl-workflow | 150 | 4.5MB | ❌ EXCEEDS LIMIT |

**Success Criteria**:
- [ ] 95% of workflows fit within 2MB
- [ ] Strategy defined for oversized workflows

**Mitigation Strategies** (if limits exceeded):
- Store large payloads in external storage (S3), pass references
- Paginate task list, load on demand via activities
- Compress state before storing
- Trim completed task payloads

**Estimated Effort**: 2 days

---

## POC Schedule

```
Week 1
├── Day 1-2: POC 1 (DeciderService Isolation)
├── Day 3-4: POC 4 (Spring @Primary) - parallel track
└── Day 5: Review POC 1 results, adjust plan

Week 2
├── Day 1-3: POC 2 (Determinism Audit)
├── Day 4-5: POC 5 (State Size Analysis) - parallel track
└── Review findings, go/no-go decision

Week 3
├── Day 1-5: POC 3 (Temporal Workflow Integration)
└── Final review, document learnings
```

## Go/No-Go Decision Points

After each POC, evaluate:

| POC | Go Condition | No-Go Action |
|-----|--------------|--------------|
| 1 | DeciderService works standalone | Evaluate rewrite vs. activity-based approach |
| 2 | No blocking non-determinism | Evaluate wrapping in activities |
| 3 | Workflow completes and replays correctly | Investigate specific failures |
| 4 | @Primary override works | Evaluate fork vs. proxy approach |
| 5 | State fits limits for target use cases | Implement pagination/external storage |

## Questions to Answer Before Starting

1. **Target Conductor version?**
   - Recommendation: 3.15.x (latest stable)

2. **Priority task types for v1?**
   - Must have: SIMPLE, HTTP, SWITCH, FORK_JOIN, DO_WHILE, SUB_WORKFLOW
   - Nice to have: WAIT, SET_VARIABLE, TERMINATE
   - Later: KAFKA_PUBLISH, JSON_JQ_TRANSFORM, HUMAN

3. **Which mode first?**
   - Recommendation: Hybrid mode (Conductor DB + Temporal execution)
   - Rationale: Simpler, validates core approach before adding complexity

4. **Real-world workflows for testing?**
   - Need 5+ production-like workflow definitions
   - Should cover various task types and complexity levels

## Success Definition

POC phase is successful if:
1. All 5 POCs complete with "Go" decision
2. Clear understanding of required workarounds
3. No architectural blockers identified
4. Estimated effort for full implementation is reasonable (<4 weeks)

## References

- [Temporal Java SDK Determinism](https://docs.temporal.io/develop/java/debugging#replay-tests)
- [Temporal Payload Size Limits](https://docs.temporal.io/dataconversion#payload-size-limits)
- [Conductor DeciderService Source](./conductor/core/src/main/java/com/netflix/conductor/core/execution/DeciderService.java)
