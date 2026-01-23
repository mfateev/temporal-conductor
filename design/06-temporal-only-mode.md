# Temporal-Only Mode: No External Dependencies

## Overview

This document evaluates the feasibility of creating a fully-featured version of Conductor that depends **only on Temporal** for all persistence needs - no Redis, PostgreSQL, or Elasticsearch required.

## Conductor DAO Requirements vs Temporal Capabilities

| DAO | Purpose | Temporal Equivalent | Feasibility |
|-----|---------|---------------------|-------------|
| **MetadataDAO** | WorkflowDef/TaskDef storage | Long-running "registry" workflow | ✅ Feasible |
| **ExecutionDAO** | Workflow/Task state | Workflow state + Visibility API | ✅ Native |
| **QueueDAO** | Task queues for workers | Native Temporal task queues | ✅ Perfect match |
| **IndexDAO** | Search/query workflows | Search Attributes + List Filters | ⚠️ Limited |
| **PollDataDAO** | Worker poll statistics | Metrics workflow or Temporal metrics | ✅ Feasible |
| **RateLimitingDAO** | Task rate limiting | Activity rate limiting in SDK | ✅ Native |
| **ConcurrentExecutionLimitDAO** | Concurrency limits | Search attributes + queries | ⚠️ Requires coordination |
| **EventHandlerDAO** | Event handler config | Config workflow or static config | ✅ Feasible |

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Conductor Server (Temporal-backed)                │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │                      REST API Layer                             │ │
│  │    (Conductor-compatible REST endpoints)                        │ │
│  └────────────────────────────────────────────────────────────────┘ │
│                              │                                       │
│  ┌───────────────────────────┼───────────────────────────────────┐  │
│  │                           │                                    │  │
│  │  ┌─────────────────┐  ┌──▼──────────────┐  ┌────────────────┐ │  │
│  │  │  MetadataDAO    │  │  ExecutionDAO   │  │   IndexDAO     │ │  │
│  │  │                 │  │                 │  │                │ │  │
│  │  │ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌────────────┐ │ │  │
│  │  │ │ Registry    │ │  │ │  Temporal   │ │  │ │ Visibility │ │ │  │
│  │  │ │ Workflow    │ │  │ │  Workflow   │ │  │ │   Queries  │ │ │  │
│  │  │ │             │ │  │ │   State     │ │  │ │            │ │ │  │
│  │  │ └─────────────┘ │  │ └─────────────┘ │  │ └────────────┘ │ │  │
│  │  └─────────────────┘  └─────────────────┘  └────────────────┘ │  │
│  │                                                                │  │
│  │  ┌─────────────────┐  ┌─────────────────┐                     │  │
│  │  │  QueueDAO       │  │  RateLimitDAO   │                     │  │
│  │  │                 │  │                 │                     │  │
│  │  │ ┌─────────────┐ │  │ ┌─────────────┐ │                     │  │
│  │  │ │  Temporal   │ │  │ │  Temporal   │ │                     │  │
│  │  │ │ Task Queues │ │  │ │  Activity   │ │                     │  │
│  │  │ │             │ │  │ │ Rate Limits │ │                     │  │
│  │  │ └─────────────┘ │  │ └─────────────┘ │                     │  │
│  │  └─────────────────┘  └─────────────────┘                     │  │
│  │                                                                │  │
│  │                 Temporal DAO Implementations                   │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                              │                                       │
└──────────────────────────────│───────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         Temporal Cluster                             │
│                                                                      │
│   Search Attributes:                                                │
│   - ConductorWorkflowType (Keyword)                                 │
│   - ConductorWorkflowVersion (Int)                                  │
│   - ConductorStatus (Keyword)                                       │
│   - ConductorCorrelationId (Keyword)                                │
│   - ConductorTags (KeywordList)                                     │
│   - ConductorStartTime (Datetime)                                   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Implementation Strategies

### 1. MetadataDAO → One Workflow Per Definition

Instead of a single registry workflow (which would be a scalability bottleneck), each definition is stored as its own workflow:

```kotlin
// Each definition is its own long-running workflow
// WorkflowId pattern: "conductor-def:workflow:{name}:{version}"
//                  or "conductor-def:task:{name}"

@WorkflowInterface
interface DefinitionWorkflow {
    @WorkflowMethod
    fun hold()  // Runs forever, just holds state

    @QueryMethod
    fun getDefinition(): Any  // Returns WorkflowDef or TaskDef

    @SignalMethod
    fun update(def: Any)
}

// Register a workflow definition
val workflowId = "conductor-def:workflow:order-processor:1"
client.newWorkflowStub(DefinitionWorkflow::class.java,
    WorkflowOptions.newBuilder()
        .setWorkflowId(workflowId)
        .setSearchAttributes(mapOf(
            "ConductorDefinitionType" to "workflow",
            "ConductorDefinitionName" to "order-processor",
            "ConductorDefinitionVersion" to 1
        ))
        .build()
)

// List all workflow definitions via visibility
client.listWorkflowExecutions("ConductorDefinitionType = 'workflow'")

// Get specific definition by direct query
val def = client.newWorkflowStub(DefinitionWorkflow::class.java, workflowId)
    .getDefinition()
```

**Benefits over single registry:**
- No single point of contention
- State distributed across workflows
- Scales to thousands of definitions
- List operations use Temporal visibility (designed for scale)

**Alternative: Pass definitions inline** - Conductor's `StartWorkflowRequest` already supports passing `WorkflowDef` directly, eliminating the need for a registry entirely. TaskDefs can be embedded.

**Caching layer** - For read performance, the Conductor server maintains an in-memory cache:

```kotlin
class TemporalMetadataDAO : MetadataDAO {
    private val cache = ConcurrentHashMap<String, Any>()

    override fun getWorkflowDef(name: String, version: Int): WorkflowDef? {
        return cache.getOrPut("workflow:$name:$version") {
            // Query the definition workflow
            client.newWorkflowStub(DefinitionWorkflow::class.java,
                "conductor-def:workflow:$name:$version"
            ).getDefinition()
        } as? WorkflowDef
    }

    override fun getAllWorkflowDefs(): List<WorkflowDef> {
        // Use visibility API - scales well
        return client.listWorkflowExecutions("ConductorDefinitionType = 'workflow'")
            .map { getWorkflowDef(it.name, it.version) }
    }
}
```

### 2. ExecutionDAO → Workflow State + Search Attributes

Each Conductor workflow maps to a Temporal workflow with custom search attributes:

```kotlin
// Set search attributes when workflow starts
workflow.upsertSearchAttributes(
    mapOf(
        "ConductorWorkflowType" to workflowDef.name,
        "ConductorStatus" to "RUNNING",
        "ConductorCorrelationId" to correlationId
    )
)

// Query running workflows via Temporal Visibility
client.listWorkflowExecutions(
    "ConductorWorkflowType = 'order-processing' AND ConductorStatus = 'RUNNING'"
)
```

### 3. QueueDAO → Native Task Queues

Temporal task queues replace Conductor's QueueDAO entirely - this is a perfect match:

```kotlin
val options = WorkflowOptions.newBuilder()
    .setTaskQueue("conductor-workflows")
    .build()
```

### 4. IndexDAO → Visibility Queries

Translate Conductor queries to Temporal List Filters:

```kotlin
class TemporalIndexDAO : IndexDAO {
    override fun searchWorkflows(query: String, ...): SearchResult<String> {
        val temporalFilter = translateToTemporalFilter(query)
        return client.listWorkflowExecutions(temporalFilter)
    }
}
```

### 5. RateLimitingDAO → Activity Rate Limiting

Temporal SDK has built-in activity rate limiting:

```kotlin
val activityOptions = ActivityOptions.newBuilder()
    .setRateLimiter(RateLimiter.create(10.0))  // 10 per second
    .build()
```

### 6. ConcurrentExecutionLimitDAO → Search Attribute Queries

Check concurrent execution count via visibility queries:

```kotlin
class TemporalConcurrentExecutionLimitDAO : ConcurrentExecutionLimitDAO {
    override fun exceedsLimit(task: TaskModel): Boolean {
        val count = client.countWorkflowExecutions(
            "ConductorTaskType = '${task.taskDefName}' AND ConductorTaskStatus = 'IN_PROGRESS'"
        )
        return count >= task.concurrencyLimit
    }
}
```

## Challenges and Limitations

| Challenge | Impact | Workaround |
|-----------|--------|------------|
| **Free-text search** | IndexDAO supports Lucene-style queries | Limited to search attributes; no full-text search on payloads |
| **Complex queries** | Conductor supports rich SQL-like queries | Temporal List Filters are simpler; may need client-side filtering |
| **Concurrent execution limits** | Cross-workflow coordination | Use visibility queries (slight race condition) or coordination workflow |
| **UI compatibility** | Conductor UI expects specific APIs | Need adapter layer or custom UI |
| **Bulk operations** | Conductor has batch APIs | Temporal CountWorkflow helps but limited batch support |
| **Query result limits** | Large result sets | Temporal visibility may be slow for >10K results |

## Feasibility Assessment

### Verdict: Feasible with Trade-offs

| Aspect | Assessment |
|--------|------------|
| **Core execution** | ✅ Fully feasible - Temporal excels here |
| **Metadata storage** | ✅ Feasible via registry workflow |
| **Task queuing** | ✅ Native Temporal feature (perfect match) |
| **Basic search** | ✅ Via search attributes |
| **Advanced search** | ⚠️ Limited - no Lucene/ES-style queries |
| **Conductor UI** | ⚠️ Needs adapter or replacement |
| **API compatibility** | ✅ REST layer can provide same endpoints |

## Recommended Approach: Two-Tier Architecture

### Tier 1: Temporal-Native (Core)
- Workflow execution, task queuing, durability
- Search attributes for basic queries
- **No external dependencies**

### Tier 2: Optional Elasticsearch (Advanced Search)
- Only if advanced search features are required
- Async indexing via activities
- Not required for core functionality

## Benefits of Temporal-Only Mode

1. **Simplified operations** - One system to manage instead of Conductor + Redis + PostgreSQL + Elasticsearch
2. **Unified durability** - Temporal handles everything with consistent guarantees
3. **Cost reduction** - No additional database infrastructure
4. **Consistent guarantees** - Temporal's exactly-once semantics everywhere
5. **Easier deployment** - Single dependency simplifies Kubernetes/cloud deployment

## Conductor UI Compatibility Analysis

**Key insight**: The Conductor UI does NOT require modifications. We can implement all the REST APIs it needs using Temporal.

### REST API Feasibility

| API Endpoint | Temporal Implementation | Status |
|--------------|------------------------|--------|
| `POST /workflow` (start) | Start Temporal workflow | ✅ Full |
| `GET /workflow/{id}` | Query workflow + reconstruct Workflow object | ✅ Full |
| `GET /workflow/running/{name}` | Visibility query on status | ✅ Full |
| `GET /workflow/{name}/correlated/{correlationId}` | Visibility query on search attribute | ✅ Full |
| `PUT /workflow/{id}/pause` | Send pause signal | ✅ Full |
| `PUT /workflow/{id}/resume` | Send resume signal | ✅ Full |
| `DELETE /workflow/{id}` (terminate) | Terminate workflow | ✅ Full |
| `POST /workflow/{id}/restart` | Start new workflow with same input | ✅ Full |
| `POST /workflow/{id}/retry` | Signal workflow to retry | ✅ Full |
| `GET /workflow/search` | Visibility query | ⚠️ Limited freeText |
| `GET /tasks/{id}` | Query workflow, find task | ✅ Full |
| `GET /tasks/{id}/log` | Query workflow memo or separate storage | ✅ Full |
| `GET /tasks/search` | Visibility query | ⚠️ Limited freeText |
| `GET /metadata/workflow` | Query registry workflow | ✅ Full |
| `POST /metadata/workflow` | Signal registry workflow | ✅ Full |

### Search Attributes Mapping

All fields in `WorkflowSummary` that the UI searches by can be mapped to Temporal search attributes:

```kotlin
// Required search attributes for full UI compatibility
val searchAttributes = mapOf(
    "ConductorWorkflowType" to workflowDef.name,           // Keyword
    "ConductorVersion" to workflowDef.version,             // Int
    "ConductorStatus" to status.name,                       // Keyword
    "ConductorCorrelationId" to correlationId,              // Keyword
    "ConductorPriority" to priority,                        // Int
    "ConductorCreatedBy" to createdBy,                      // Keyword
    "ConductorIdempotencyKey" to idempotencyKey,            // Keyword
    "ConductorFailedTaskNames" to failedTaskNames,          // KeywordList
)
// Built-in Temporal attributes also available:
// - WorkflowId, WorkflowType, StartTime, CloseTime, ExecutionStatus
```

### What Works Without Changes

| UI Feature | How It Works |
|------------|--------------|
| **Workflow list** | Visibility query: `ConductorWorkflowType = 'X' AND ExecutionStatus = 'Running'` |
| **Workflow details** | Query workflow, reconstruct full `Workflow` object from state |
| **Task list within workflow** | Tasks stored in workflow state, returned with workflow |
| **Filter by status** | `ExecutionStatus = 'Running'` / `'Completed'` / `'Failed'` |
| **Filter by time range** | `StartTime > '2024-01-01' AND StartTime < '2024-01-02'` |
| **Filter by correlation ID** | `ConductorCorrelationId = 'order-123'` |
| **Pause/Resume/Terminate** | Temporal signals and termination |
| **Retry/Restart** | Start new workflow or signal current |
| **View input/output** | Stored in workflow state, queryable |

### What Has Limitations

| UI Feature | Limitation | Workaround |
|------------|------------|------------|
| **Free-text search** | Temporal doesn't support Lucene-style `freeText` | Return empty results for freeText, support structured `query` only |
| **Search in payload** | Can't search inside input/output JSON | Add key fields as search attributes if needed |
| **Task-level search** | Tasks aren't indexed separately | Search by workflow, filter client-side |
| **Very large result sets** | Visibility API slower for >10K results | Pagination works fine for UI use cases |

### Implementation: Search Query Translation

```kotlin
class TemporalWorkflowService : WorkflowService {

    override fun searchWorkflows(
        start: Int, size: Int, sort: String?,
        freeText: String?, query: String?
    ): SearchResult<WorkflowSummary> {

        // Translate Conductor query to Temporal List Filter
        // Conductor: "status IN (RUNNING) AND workflowType = 'order'"
        // Temporal:  "ExecutionStatus = 'Running' AND ConductorWorkflowType = 'order'"

        val temporalFilter = translateQuery(query)

        val executions = temporalClient.listWorkflowExecutions(
            ListWorkflowExecutionsRequest.newBuilder()
                .setQuery(temporalFilter)
                .setPageSize(size)
                .build()
        )

        // Convert to WorkflowSummary
        return SearchResult(
            totalHits = executions.size.toLong(),
            results = executions.map { toWorkflowSummary(it) }
        )
    }

    private fun translateQuery(conductorQuery: String?): String {
        // Map Conductor fields to Temporal search attributes
        return conductorQuery
            ?.replace("workflowType", "ConductorWorkflowType")
            ?.replace("status", "ExecutionStatus")
            ?.replace("correlationId", "ConductorCorrelationId")
            // ... etc
            ?: ""
    }
}
```

### Verdict: UI Works Without Modification

The Conductor UI can work unchanged because:

1. **All core APIs are implementable** - Start, get, list, pause, resume, terminate, retry
2. **Structured search works** - Filter by status, type, time, correlationId
3. **freeText is optional** - UI still functions if freeText returns no results
4. **Workflow details fully available** - All data stored in Temporal workflow state

**The only limitation**: Users cannot do free-text search across payload data. This is a power-user feature that most users don't rely on.

## When NOT to Use Temporal-Only Mode

- Need Lucene-style full-text search on workflow payload data
- Need complex cross-workflow analytics beyond basic filters
- High-volume search queries (>10K results frequently)
- Need to query by arbitrary payload fields (not pre-defined search attributes)

## References

- [Temporal Visibility](https://docs.temporal.io/visibility)
- [Temporal Search Attributes](https://docs.temporal.io/search-attribute)
- [Temporal List Filters](https://docs.temporal.io/list-filter)
