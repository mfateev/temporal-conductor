# Conductor Concurrency Model

## No Database Transactions

Conductor does **not** use database transactions (`@Transactional`). Operations are independent:

```java
executionDAOFacade.updateTasks(workflow.getTasks());
executionDAOFacade.updateWorkflow(workflow);
```

## Distributed Locking

Instead of transactions, Conductor uses distributed locking per workflow:

```java
// WorkflowExecutorOps.java
public WorkflowModel decide(String workflowId) {
    boolean lockAcquired = executionLockService.acquireLock(workflowId);
    if (!lockAcquired) return null;
    try {
        return decide(executionDAOFacade.getWorkflowModel(workflowId, true));
    } finally {
        executionLockService.releaseLock(workflowId);
    }
}
```

This ensures only one thread/process modifies a workflow at a time.

## Implications for Temporal Integration

In a Temporal workflow context:

| Concern | Conductor Server | Temporal Workflow |
|---------|------------------|-------------------|
| Concurrent access | Multiple workers → needs locking | Single-threaded → no locking needed |
| Durability | Database persistence | Temporal event history |
| Crash recovery | Re-read from database | Replay from event history |
| Cross-workflow queries | Index/search across workflows | Not needed (single workflow) |

**Key insight**: Temporal's single-threaded workflow execution model eliminates the need for distributed locking that Conductor requires. This simplifies our DAO implementations significantly.
