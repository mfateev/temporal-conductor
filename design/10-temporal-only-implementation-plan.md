# Temporal-Only Mode: Implementation Plan

## Overview

This plan implements a Conductor-compatible server backed entirely by Temporal, with no external database dependencies. The Conductor UI works unchanged by implementing Conductor's REST APIs using Temporal's workflow state and visibility APIs.

**Key Insight**: The Conductor UI is a React SPA that depends **only on the REST API**. This enables a REST-first approach where we validate UI compatibility early before building the full backend.

## Architecture Target

```
┌─────────────────────────────────────────────────────────────────────┐
│                 Conductor Server (Temporal-backed)                   │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │              Conductor-Compatible REST API                      │ │
│  │   /api/workflow, /api/metadata, /api/tasks, /api/event         │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                              │                                       │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │                    Service Layer                                │ │
│  │   WorkflowService, MetadataService, TaskService                │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                              │                                       │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │                 Temporal DAO Implementations                    │ │
│  │                                                                 │ │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐            │ │
│  │  │TemporalMeta- │ │TemporalExec- │ │TemporalIndex │            │ │
│  │  │  dataDAO     │ │  utionDAO    │ │    DAO       │            │ │
│  │  └──────────────┘ └──────────────┘ └──────────────┘            │ │
│  │                                                                 │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                              │                                       │
└──────────────────────────────│───────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        Temporal Cluster                              │
│                                                                      │
│  ┌─────────────────────┐  ┌─────────────────────────────────────┐  │
│  │  ConductorWorkflow  │  │        Visibility Store              │  │
│  │  (execution engine) │  │  (search attributes for queries)    │  │
│  └─────────────────────┘  └─────────────────────────────────────┘  │
│                                                                      │
│  Search Attributes:                                                  │
│  - ConductorWorkflowType (Keyword)                                  │
│  - ConductorWorkflowVersion (Int)                                   │
│  - ConductorStatus (Keyword)                                        │
│  - ConductorCorrelationId (Keyword)                                 │
│  - ConductorPriority (Int)                                          │
│  - ConductorFailedTaskNames (KeywordList)                           │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: Workflow Observability

**Goal**: Make workflow state queryable for UI integration.

### 1.1 Add Search Attributes to ConductorWorkflowImpl

Update workflow to set Temporal search attributes for visibility queries:

- `ConductorWorkflowType` - Workflow definition name
- `ConductorWorkflowVersion` - Workflow definition version
- `ConductorStatus` - RUNNING, COMPLETED, FAILED, PAUSED, TERMINATED
- `ConductorCorrelationId` - Correlation ID for grouping
- `ConductorPriority` - Workflow priority
- `ConductorFailedTaskNames` - List of failed task names (for filtering)

### 1.2 Add Query Methods to ConductorWorkflow

Add `@QueryMethod` annotations for synchronous state access:

```kotlin
@QueryMethod
fun getFullState(): ConductorWorkflowState  // Full workflow + tasks

@QueryMethod
fun getTaskStates(): List<TaskState>  // Just task information
```

### 1.3 Add Signal Methods for Control

Add `@SignalMethod` for workflow control operations:

```kotlin
@SignalMethod
fun pause()

@SignalMethod
fun resume()

@SignalMethod
fun retryFailedTask(taskRefName: String)
```

### Deliverables
- Updated `ConductorWorkflowImpl` with search attributes
- Query methods for workflow state
- Signal methods for pause/resume/retry
- Tests verifying query and signal functionality

---

## Phase 2: REST API Layer (with Stub Backend)

**Goal**: Implement Conductor-compatible REST endpoints with stub/mock implementations to enable early UI validation.

### 2.1 Project Setup

Create new module: `conductor-server-temporal`

```
conductor-server-temporal/
├── src/main/kotlin/
│   ├── api/                    # REST controllers
│   │   ├── WorkflowResource.kt
│   │   ├── MetadataResource.kt
│   │   └── TaskResource.kt
│   ├── dto/                    # Request/Response DTOs
│   │   ├── StartWorkflowRequest.kt
│   │   ├── WorkflowSummary.kt
│   │   └── SearchResult.kt
│   ├── service/                # Service interfaces + stubs
│   │   ├── WorkflowService.kt
│   │   ├── StubWorkflowService.kt
│   │   └── ...
│   └── Application.kt
└── src/test/kotlin/
    └── api/                    # API contract tests
```

### 2.2 Workflow Endpoints

| Endpoint | Method | Stub Behavior |
|----------|--------|---------------|
| `/api/workflow` | POST | Return generated workflow ID |
| `/api/workflow/{workflowId}` | GET | Return mock workflow with tasks |
| `/api/workflow/{workflowId}/status` | GET | Return mock status |
| `/api/workflow/{workflowId}` | DELETE | Return success |
| `/api/workflow/{workflowId}/pause` | PUT | Return success |
| `/api/workflow/{workflowId}/resume` | PUT | Return success |
| `/api/workflow/{workflowId}/restart` | POST | Return new workflow ID |
| `/api/workflow/{workflowId}/retry` | POST | Return success |
| `/api/workflow/running/{name}` | GET | Return mock list |
| `/api/workflow/search` | GET | Return mock search results |

### 2.3 Metadata Endpoints

| Endpoint | Method | Stub Behavior |
|----------|--------|---------------|
| `/api/metadata/workflow` | GET | Return mock definition list |
| `/api/metadata/workflow/{name}` | GET | Return mock definition |
| `/api/metadata/workflow` | POST | Return success |
| `/api/metadata/workflow` | PUT | Return success |
| `/api/metadata/workflow/{name}/{version}` | DELETE | Return success |
| `/api/metadata/taskdefs` | GET | Return mock task defs |
| `/api/metadata/taskdefs` | POST | Return success |

### 2.4 Task Endpoints

| Endpoint | Method | Stub Behavior |
|----------|--------|---------------|
| `/api/tasks/{taskId}` | GET | Return mock task |
| `/api/tasks/search` | GET | Return mock results |
| `/api/tasks/{taskId}/log` | POST | Return success |
| `/api/tasks/{taskId}/log` | GET | Return mock logs |

### 2.5 Stub Data Generator

Create realistic mock data that exercises UI features:

```kotlin
object StubDataGenerator {
    fun createWorkflow(id: String, status: String): Workflow
    fun createWorkflowWithTasks(taskCount: Int): Workflow
    fun createSearchResults(count: Int): SearchResult<WorkflowSummary>
    fun createWorkflowDef(name: String): WorkflowDef
}
```

### Deliverables
- Spring Boot application with all REST endpoints
- Stub service implementations with realistic mock data
- Request/response DTOs matching Conductor API exactly
- OpenAPI/Swagger documentation
- API contract tests

---

## Phase 2.5: UI Validation Checkpoint

**Goal**: Validate Conductor UI works with our REST API before building real backend.

### 2.5.1 UI Integration Test

1. Run `conductor-server-temporal` with stub backend
2. Point Conductor UI at our server
3. Validate all UI features work:

| UI Feature | Validation |
|------------|------------|
| Workflow list | Displays stub workflows |
| Workflow details | Shows tasks, input, output |
| Search by status | Filter controls work |
| Search by type | Filter controls work |
| Search by time | Date picker works |
| Pagination | Next/prev pages work |
| Pause/Resume buttons | API calls succeed |
| Terminate button | API calls succeed |
| Retry/Restart | API calls succeed |
| Metadata view | Shows definitions |

### 2.5.2 API Compatibility Report

Document any gaps or issues discovered:
- Missing endpoints the UI expects
- Response format differences
- Required fields we didn't include
- Search query format requirements

### 2.5.3 Fix Compatibility Issues

Address any issues found before proceeding to real implementation.

### Deliverables
- UI compatibility test report
- List of API adjustments needed
- Updated REST controllers if needed
- Confidence that UI will work with real backend

---

## Phase 3: Temporal DAO Implementations

**Goal**: Implement Conductor DAO interfaces using Temporal APIs.

### 3.1 TemporalExecutionDAO

Implements `ExecutionDAO` interface:

| Method | Temporal Implementation |
|--------|------------------------|
| `getWorkflow(id)` | Query workflow via `@QueryMethod` |
| `getRunningWorkflowIds(name)` | Visibility: `ConductorWorkflowType = X AND ExecutionStatus = 'Running'` |
| `getWorkflowsByCorrelationId(id)` | Visibility: `ConductorCorrelationId = X` |
| `createWorkflow(model)` | Start Temporal workflow |
| `updateWorkflow(model)` | Signal workflow (if needed) |
| `removeWorkflow(id)` | Terminate workflow |

### 3.2 TemporalIndexDAO

Implements `IndexDAO` interface:

| Method | Temporal Implementation |
|--------|------------------------|
| `searchWorkflows(query)` | Translate to Visibility List Filter |
| `searchWorkflowSummary(query)` | Visibility query + map to `WorkflowSummary` |
| `getWorkflowCount(query)` | Visibility count query |

Query translation layer:
```kotlin
class ConductorQueryTranslator {
    // Conductor: "workflowType = 'order' AND status IN (RUNNING, PAUSED)"
    // Temporal:  "ConductorWorkflowType = 'order' AND ExecutionStatus IN ('Running', 'Paused')"
    fun translate(conductorQuery: String): String
}
```

Field mappings:
- `workflowType` → `ConductorWorkflowType`
- `status` → `ExecutionStatus` (with value translation)
- `correlationId` → `ConductorCorrelationId`
- `startTime` → `StartTime`
- `updateTime` → `CloseTime` (approximate)

### 3.3 TemporalMetadataDAO

**Option A: In-Memory with Workflow Backup** (Recommended for simplicity)

```kotlin
class TemporalMetadataDAO : MetadataDAO {
    // In-memory cache for fast reads
    private val workflowDefs = ConcurrentHashMap<String, WorkflowDef>()
    private val taskDefs = ConcurrentHashMap<String, TaskDef>()

    // Persist to Temporal workflow for durability
    private val registryWorkflow: MetadataRegistryWorkflow

    override fun createWorkflowDef(def: WorkflowDef) {
        workflowDefs["${def.name}:${def.version}"] = def
        registryWorkflow.updateWorkflowDef(def)  // Signal for durability
    }
}
```

**Option B: Definition Workflows** (More scalable)
- Each definition stored as a long-running workflow
- WorkflowId: `conductor-def:workflow:{name}:{version}`
- Better for large numbers of definitions

### Deliverables
- `TemporalExecutionDAO` implementation
- `TemporalIndexDAO` implementation with query translator
- `TemporalMetadataDAO` implementation
- Unit tests for each DAO
- Integration tests with Temporal test server

---

## Phase 4: Service Layer Integration

**Goal**: Connect REST API to real Temporal backend by replacing stubs with real services.

### 4.1 TemporalWorkflowService

Replace `StubWorkflowService` with real implementation:

```kotlin
@Service
class TemporalWorkflowService(
    private val temporalClient: WorkflowClient,
    private val executionDAO: TemporalExecutionDAO,
    private val indexDAO: TemporalIndexDAO,
    private val metadataDAO: TemporalMetadataDAO
) : WorkflowService {

    override fun startWorkflow(request: StartWorkflowRequest): String {
        val workflowDef = metadataDAO.getWorkflowDef(request.name, request.version)
        val options = WorkflowOptions.newBuilder()
            .setWorkflowId(request.workflowId ?: UUID.randomUUID().toString())
            .setTaskQueue("conductor-workflows")
            .setSearchAttributes(buildSearchAttributes(request, workflowDef))
            .build()

        val workflow = temporalClient.newWorkflowStub(ConductorWorkflow::class.java, options)
        WorkflowClient.start(workflow::execute, toConductorInput(workflowDef, request))
        return options.workflowId
    }

    override fun getExecutionStatus(workflowId: String, includeTasks: Boolean): Workflow {
        return executionDAO.getWorkflow(workflowId, includeTasks)
    }

    override fun searchWorkflows(query: String, freeText: String?, start: Int, size: Int): SearchResult {
        return indexDAO.searchWorkflows(query, freeText, start, size)
    }

    // ... other methods
}
```

### 4.2 TemporalMetadataService

Replace `StubMetadataService` with real implementation:

```kotlin
@Service
class TemporalMetadataService(
    private val metadataDAO: TemporalMetadataDAO
) : MetadataService {

    override fun registerWorkflowDef(def: WorkflowDef) {
        metadataDAO.createWorkflowDef(def)
    }

    override fun getWorkflowDef(name: String, version: Int): WorkflowDef {
        return metadataDAO.getWorkflowDef(name, version)
            ?: throw NotFoundException("Workflow $name:$version not found")
    }

    // ... other methods
}
```

### 4.3 Configuration

```kotlin
@Configuration
class TemporalConfiguration {
    @Bean
    fun workflowClient(): WorkflowClient {
        return WorkflowClient.newInstance(
            WorkflowServiceStubs.newLocalServiceStubs(),
            WorkflowClientOptions.newBuilder()
                .setNamespace("conductor")
                .build()
        )
    }

    @Bean
    @Profile("!stub")  // Use real service except in stub profile
    fun workflowService(client: WorkflowClient, ...): WorkflowService {
        return TemporalWorkflowService(client, ...)
    }

    @Bean
    @Profile("stub")  // Use stub for UI testing
    fun stubWorkflowService(): WorkflowService {
        return StubWorkflowService()
    }
}
```

### Deliverables
- `TemporalWorkflowService` implementation
- `TemporalMetadataService` implementation
- Spring configuration for switching stub/real
- Unit tests with mocked Temporal client
- Integration tests with Temporal test server

---

## Phase 5: Full Integration Testing

**Goal**: Validate complete system end-to-end.

### 5.1 End-to-End Test Suite

```kotlin
@SpringBootTest
@Testcontainers
class EndToEndTest {
    @Container
    val temporal = TemporalContainer()

    @Test
    fun `workflow lifecycle - start, query, complete`()

    @Test
    fun `workflow search by status`()

    @Test
    fun `workflow search by type and time range`()

    @Test
    fun `pause and resume workflow`()

    @Test
    fun `terminate workflow`()

    @Test
    fun `metadata CRUD operations`()
}
```

### 5.2 Conductor UI Full Validation

Re-run UI validation from Phase 2.5 with real backend:

| UI Feature | Test with Real Backend |
|------------|----------------------|
| Start workflow | Actually starts Temporal workflow |
| Workflow list | Shows real running workflows |
| Workflow details | Shows real task execution |
| Search | Queries Temporal visibility |
| Pause/Resume | Signals real workflow |
| Terminate | Terminates real workflow |

### 5.3 Performance Testing

| Test | Target |
|------|--------|
| Workflow start latency | < 100ms |
| Workflow query latency | < 50ms |
| Search with 10K workflows | < 500ms |
| Search with 100K workflows | < 2s |

### Deliverables
- Comprehensive end-to-end test suite
- UI compatibility verification (real backend)
- Performance benchmark results
- Known limitations documentation

---

## Phase 6: Production Readiness

**Goal**: Prepare for production deployment.

### 6.1 Configuration

- Temporal connection settings
- Search attribute registration scripts
- Task queue configuration
- Retry policies and timeouts

### 6.2 Monitoring

- Temporal metrics integration
- Custom metrics for Conductor concepts
- Health check endpoints
- Alerting rules

### 6.3 Documentation

- Deployment guide
- Configuration reference
- API compatibility matrix
- Migration guide (from Conductor)
- Known limitations

### Deliverables
- Production configuration templates
- Helm charts / Kubernetes manifests
- Monitoring dashboards (Grafana)
- Operations runbook

---

## Summary

| Phase | Focus | Key Deliverable |
|-------|-------|-----------------|
| **1** | Workflow Observability | Search attributes + query methods |
| **2** | REST API (Stubs) | Conductor-compatible endpoints with mock data |
| **2.5** | UI Validation | Confirm UI works before building backend |
| **3** | Temporal DAOs | ExecutionDAO, IndexDAO, MetadataDAO |
| **4** | Service Integration | Connect REST to real Temporal backend |
| **5** | Full Integration | End-to-end testing with UI |
| **6** | Production | Deployment & monitoring |

## Dependencies (REST-First Approach)

```
Phase 1: Workflow Observability
    │
    ├──────────────────────────────────┐
    │                                  │
    ▼                                  ▼
Phase 2: REST API (Stubs)         Phase 3: Temporal DAOs
    │                                  │
    ▼                                  │
Phase 2.5: UI Validation               │
    │                                  │
    │   ┌──────────────────────────────┘
    │   │
    ▼   ▼
Phase 4: Service Integration
    │
    ▼
Phase 5: Full Integration
    │
    ▼
Phase 6: Production
```

**Benefits of REST-First:**
1. Early UI validation before complex backend work
2. Clear API contract defined upfront
3. Parallel development of DAOs while testing UI
4. Fast feedback on compatibility issues
5. Stub mode useful for UI development/demos

## Estimated Effort

| Phase | Complexity | Notes |
|-------|------------|-------|
| 1 | Low | Extend existing workflow |
| 2 | Medium | Many endpoints, realistic stubs |
| 2.5 | Low | Testing checkpoint |
| 3 | Medium | Query translation is the challenge |
| 4 | Low | Wire up existing pieces |
| 5 | Medium | Integration testing takes time |
| 6 | Low-Medium | Standard production prep |

## Open Questions

1. **Metadata storage**: In-memory + backup workflow vs. per-definition workflows?
2. **Task-level search**: How important is searching by task fields?
3. **Free-text search**: Accept limitation or add optional Elasticsearch?
4. **Multi-tenancy**: Namespace per tenant or shared namespace?
5. **Backward compatibility**: Support for existing Conductor workers?
