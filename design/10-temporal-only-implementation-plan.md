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

## Phase 3: Query Translation & Visibility Integration

> **Architecture Note**: The original plan called for `TemporalExecutionDAO` and `TemporalIndexDAO`
> implementations. However, the current architecture **does not need separate DAOs** because:
> 1. The service layer (`TemporalWorkflowService`, etc.) talks directly to Temporal via `WorkflowClient`
> 2. Workflow state is maintained in-memory within the workflow, exposed via query methods
> 3. Temporal provides durability through event history replay
>
> Instead, this phase focuses on enhancing the existing service layer with query translation
> and Temporal visibility API integration.

**Goal**: Enable real workflow search by integrating Temporal's visibility API.

### 3.1 Query Translation Layer

Add `ConductorQueryTranslator` to translate Conductor query syntax to Temporal visibility queries:

```java
@Component
public class ConductorQueryTranslator {
    // Conductor: "workflowType = 'order' AND status IN (RUNNING, PAUSED)"
    // Temporal:  "ConductorWorkflowType = 'order' AND ExecutionStatus IN ('Running', 'Paused')"
    public String translate(String conductorQuery);
}
```

Field mappings:
- `workflowType` → `ConductorWorkflowType`
- `status` → `ExecutionStatus` (with value translation: RUNNING→Running, COMPLETED→Completed)
- `correlationId` → `ConductorCorrelationId`
- `startTime` → `StartTime`
- `updateTime` → `CloseTime` (approximate)

### 3.2 Visibility API Integration

Enhance `TemporalWorkflowService.searchWorkflows()` to use Temporal visibility:

```java
@Override
public SearchResult<WorkflowSummary> searchWorkflows(String query, String freeText, int start, int size) {
    String temporalQuery = queryTranslator.translate(query);

    ListWorkflowExecutionsRequest request = ListWorkflowExecutionsRequest.newBuilder()
        .setNamespace(namespace)
        .setQuery(temporalQuery)
        .setPageSize(size)
        .build();

    ListWorkflowExecutionsResponse response = workflowServiceStubs
        .blockingStub()
        .listWorkflowExecutions(request);

    List<WorkflowSummary> results = response.getExecutionsList().stream()
        .map(this::toWorkflowSummary)
        .collect(toList());

    return new SearchResult<>(results, response.getExecutionsCount());
}
```

### 3.3 Metadata Storage (Already Implemented)

Metadata is stored in-memory with the service layer:
- `TemporalMetadataService` maintains `ConcurrentHashMap` for workflow/task definitions
- Definitions are passed to workflows at start time via `ConductorWorkflowInput`
- No separate persistence needed (definitions can be re-registered on restart)

### Deliverables
- `ConductorQueryTranslator` implementation
- Updated `TemporalWorkflowService.searchWorkflows()` with visibility API
- Updated `TemporalWorkflowService.getRunningWorkflows()` with visibility API
- Unit tests for query translation
- Integration tests with Temporal test server

---

## Phase 4: Remaining API Endpoints

**Goal**: Implement any remaining API endpoints not yet functional.

### 4.1 Current Status

Already implemented and working:
- Start/Get/Terminate/Pause/Resume workflow
- Restart/Retry workflow
- Get workflow status
- Search workflows (stub - needs visibility integration from Phase 3)
- All metadata CRUD operations
- Task get/update/poll operations

### 4.2 Remaining Endpoints

| Endpoint | Status | Notes |
|----------|--------|-------|
| `POST /api/workflow/{id}/rerun` | Not implemented | Rerun completed workflow |
| `POST /api/workflow/{id}/skiptask/{ref}` | Not implemented | Skip a task |
| Real search (not stub) | Phase 3 | Needs query translation |

### 4.3 TemporalWorkflowService (Current Architecture)

The service layer talks directly to Temporal without DAOs:

```java
@Service
public class TemporalWorkflowService implements WorkflowService {
    private final WorkflowClient workflowClient;
    private final WorkflowServiceStubs workflowServiceStubs;
    private final MetadataService metadataService;

    // Start workflow: WorkflowClient.newWorkflowStub().start()
    // Get workflow: Query via workflow stub
    // Pause/Resume: Signal via workflow stub
    // Search: Visibility API (after Phase 3)
}
```

### Deliverables
- Rerun workflow endpoint
- Skip task endpoint (if needed)
- Integration of Phase 3 query translation
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

| Phase | Focus | Key Deliverable | Status |
|-------|-------|-----------------|--------|
| **1** | Workflow Observability | Search attributes + query methods | ✅ Done |
| **2** | REST API (Stubs) | Conductor-compatible endpoints with mock data | ✅ Done |
| **2.5** | UI Validation | Confirm UI works before building backend | Pending |
| **3** | Query Translation | Visibility API integration for real search | Pending |
| **4** | Remaining Endpoints | Rerun, skip task, etc. | Pending |
| **5** | Full Integration | End-to-end testing with UI | Pending |
| **6** | Production | Deployment & monitoring | Pending |

> **Note**: The original Phase 3 (Temporal DAOs) was removed. The service layer
> talks directly to Temporal via `WorkflowClient` - no intermediate DAO layer needed.
> Workflow state is maintained in-memory within the workflow itself.

## Dependencies

```
Phase 1: Workflow Observability     ✅ DONE
    │
    ▼
Phase 2: REST API (Stubs)           ✅ DONE
    │
    ▼
Phase 2.5: UI Validation            ← Next step
    │
    ▼
Phase 3: Query Translation          ← Enables real search
    │
    ▼
Phase 4: Remaining Endpoints
    │
    ▼
Phase 5: Full Integration
    │
    ▼
Phase 6: Production
```

**Current Architecture:**
- REST API → Service Layer → Temporal WorkflowClient → Workflow (in-memory state)
- No DAOs needed - services talk directly to Temporal
- Workflow maintains all state, exposed via query methods
- Temporal provides durability through event history replay

## Estimated Effort

| Phase | Complexity | Notes |
|-------|------------|-------|
| 1 | Low | ✅ Complete |
| 2 | Medium | ✅ Complete |
| 2.5 | Low | Testing checkpoint |
| 3 | Medium | Query translation is the main work |
| 4 | Low | Just a few endpoints |
| 5 | Medium | Integration testing takes time |
| 6 | Low-Medium | Standard production prep |

## Open Questions

1. **Metadata storage**: In-memory + backup workflow vs. per-definition workflows?
2. **Task-level search**: How important is searching by task fields?
3. **Free-text search**: Accept limitation or add optional Elasticsearch?
4. **Multi-tenancy**: Namespace per tenant or shared namespace?
5. **Backward compatibility**: Support for existing Conductor workers?
