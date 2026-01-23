# Conductor UI Integration

## Goal

Enable workflows started from the Conductor UI to execute on Temporal, while maintaining full visibility in Conductor's native UI and APIs.

## Approach: Spring Bean Override (No Conductor Modifications)

Conductor uses Spring's `@ComponentScan` for bean discovery:

```java
// Conductor.java (main application)
@ComponentScan(basePackages = {"com.netflix.conductor", "io.orkes.conductor"})
public class Conductor { ... }
```

The `WorkflowServiceImpl` has a plain `@Service` annotation (no `@ConditionalOnMissingBean`), which means we can override it using Spring's `@Primary` annotation in our custom module:

```kotlin
// In our temporal-conductor module
package com.temporal.conductor.service

@Service
@Primary  // Takes precedence over WorkflowServiceImpl
class TemporalWorkflowService(
    private val temporalClient: WorkflowClient,
    private val metadataService: MetadataService,
    private val executionDAOFacade: ExecutionDAOFacade  // For visibility sync
) : WorkflowService {

    override fun startWorkflow(request: StartWorkflowRequest): String {
        // 1. Validate workflow definition
        val workflowDef = metadataService.getWorkflowDef(request.name, request.version)

        // 2. Start Temporal workflow
        val workflowId = request.workflowId ?: UUID.randomUUID().toString()
        val options = WorkflowOptions.newBuilder()
            .setWorkflowId(workflowId)
            .setTaskQueue("conductor-workflows")
            .build()

        val workflow = temporalClient.newWorkflowStub(
            ConductorWorkflow::class.java, options
        )
        WorkflowClient.start(workflow::execute, workflowDef, request.input)

        return workflowId
    }

    // getWorkflow, getRunningWorkflows, etc. can query Conductor DB
    // (synced by activities) or Temporal directly
}
```

## Deployment Model

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Custom Conductor Server                           │
│                                                                      │
│   ┌──────────────────┐    ┌──────────────────────────────────────┐  │
│   │  Conductor Core  │    │  temporal-conductor module            │  │
│   │  (unmodified)    │    │                                       │  │
│   │                  │    │  @Primary TemporalWorkflowService    │  │
│   │  - MetadataDAO   │    │  - Routes startWorkflow to Temporal  │  │
│   │  - ExecutionDAO  │    │  - Syncs state back to Conductor DB  │  │
│   │  - REST APIs     │    │                                       │  │
│   └──────────────────┘    └──────────────────────────────────────┘  │
│            │                            │                            │
│            ▼                            ▼                            │
│   ┌──────────────────┐         ┌──────────────────┐                 │
│   │  Conductor UI    │         │  Temporal Client │                 │
│   │  (visibility)    │         │  (execution)     │                 │
│   └──────────────────┘         └──────────────────┘                 │
│                                         │                            │
└─────────────────────────────────────────│────────────────────────────┘
                                          │
                                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         Temporal Cluster                             │
│                                                                      │
│   ┌───────────────────────────────────────────────────────────────┐ │
│   │                    ConductorWorkflow                           │ │
│   │   - In-memory HashMap DAOs                                     │ │
│   │   - DeciderService scheduling                                  │ │
│   │   - Activities for task execution                              │ │
│   │   - Sync activities to update Conductor DB                     │ │
│   └───────────────────────────────────────────────────────────────┘ │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Visibility Sync

The Temporal workflow periodically syncs state back to Conductor's database using activities:

```kotlin
// SyncActivities.kt
@ActivityInterface
interface ConductorSyncActivities {
    fun syncWorkflowStatus(workflowId: String, status: WorkflowModel)
    fun syncTaskStatus(workflowId: String, tasks: List<TaskModel>)
}
```

This enables:
- **Conductor UI**: Shows workflow progress, task states, logs
- **Conductor APIs**: Query workflow status, search, etc.
- **Conductor Metrics**: Native monitoring and alerting

## Key Benefits

1. **No Conductor modifications** - Standard Conductor release + our module
2. **Full UI compatibility** - Native Conductor UI works unchanged
3. **Gradual migration** - Can run some workflows on Conductor, others on Temporal
4. **Temporal execution benefits** - Durability, replay, versioning
