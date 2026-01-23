package io.temporal.conductor.ext

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.conductor.common.metadata.events.EventExecution
import com.netflix.conductor.common.metadata.tasks.PollData
import com.netflix.conductor.common.run.SearchResult
import com.netflix.conductor.common.run.TaskSummary
import com.netflix.conductor.common.run.Workflow
import com.netflix.conductor.common.run.WorkflowSummary
import com.netflix.conductor.core.config.ConductorProperties
import com.netflix.conductor.core.dal.ExecutionDAOFacade
import com.netflix.conductor.core.events.queue.Message
import com.netflix.conductor.core.utils.ExternalPayloadStorageUtils
import com.netflix.conductor.dao.*
import com.netflix.conductor.model.TaskModel
import com.netflix.conductor.model.WorkflowModel
import java.util.concurrent.CompletableFuture

/**
 * In-memory ExecutionDAOFacade for use inside Temporal workflows.
 *
 * Temporal workflows have durable memory - any state in the workflow is automatically
 * persisted and replayed. This means we don't need external database persistence.
 *
 * This class provides no-op implementations for methods that would normally
 * persist to a database:
 * - updateWorkflow(): Used by SetVariable - no-op since Temporal handles persistence
 * - removeTask(): Used by DoWhile (keepLastN cleanup) - no-op since Temporal manages history
 */
class InMemoryExecutionDAOFacade(
    objectMapper: ObjectMapper = ObjectMapper()
) : ExecutionDAOFacade(
    NoOpExecutionDAO(),
    NoOpQueueDAO(),
    NoOpIndexDAO(),
    NoOpRateLimitingDAO(),
    NoOpConcurrentExecutionLimitDAO(),
    NoOpPollDataDAO(),
    objectMapper,
    ConductorProperties(),
    NoOpExternalPayloadStorageUtils()
) {
    /**
     * No-op: Temporal automatically persists workflow state.
     * Called by SetVariable after updating workflow variables.
     */
    override fun updateWorkflow(workflowModel: WorkflowModel): String {
        // No-op - Temporal handles persistence automatically
        return workflowModel.workflowId
    }

    /**
     * No-op: Temporal manages history size differently.
     * Called by DoWhile for keepLastN cleanup of old iterations.
     */
    override fun removeTask(taskId: String) {
        // No-op - Temporal handles history management
    }
}

// Stub implementations of required DAOs

private class NoOpExecutionDAO : ExecutionDAO {
    override fun getPendingWorkflowsByType(workflowName: String, version: Int): MutableList<WorkflowModel> = mutableListOf()
    override fun getWorkflowsByType(workflowName: String, startTime: Long?, endTime: Long?): MutableList<WorkflowModel> = mutableListOf()
    override fun getWorkflowsByCorrelationId(workflowName: String, correlationId: String, includeTasks: Boolean): MutableList<WorkflowModel> = mutableListOf()
    override fun getWorkflow(workflowId: String): WorkflowModel? = null
    override fun getWorkflow(workflowId: String, includeTasks: Boolean): WorkflowModel? = null
    override fun getRunningWorkflowIds(workflowName: String, version: Int): MutableList<String> = mutableListOf()
    override fun getPendingWorkflowCount(workflowName: String): Long = 0
    override fun createWorkflow(workflow: WorkflowModel): String = workflow.workflowId
    override fun updateWorkflow(workflow: WorkflowModel): String = workflow.workflowId
    override fun removeWorkflow(workflowId: String): Boolean = true
    override fun removeWorkflowWithExpiry(workflowId: String, ttlSeconds: Int): Boolean = true
    override fun removeFromPendingWorkflow(workflowType: String, workflowId: String) {}
    override fun getTask(taskId: String): TaskModel? = null
    override fun getTasks(taskType: String, startKey: String?, count: Int): MutableList<TaskModel> = mutableListOf()
    override fun getTasks(taskIds: MutableList<String>): MutableList<TaskModel> = mutableListOf()
    override fun createTasks(tasks: MutableList<TaskModel>): MutableList<TaskModel> = tasks
    override fun updateTask(task: TaskModel) {}
    override fun getTasksForWorkflow(workflowId: String): MutableList<TaskModel> = mutableListOf()
    override fun removeTask(taskId: String): Boolean = true
    override fun getPendingTasksForTaskType(taskType: String): MutableList<TaskModel> = mutableListOf()
    override fun getInProgressTaskCount(taskDefName: String): Long = 0
    override fun addEventExecution(eventExecution: EventExecution): Boolean = true
    override fun updateEventExecution(eventExecution: EventExecution) {}
    override fun removeEventExecution(eventExecution: EventExecution) {}
    override fun canSearchAcrossWorkflows(): Boolean = false
    override fun getPendingTasksByWorkflow(workflowId: String, taskName: String): MutableList<TaskModel> = mutableListOf()
}

private class NoOpQueueDAO : QueueDAO {
    override fun push(queueName: String, id: String, offsetTimeInSecond: Long) {}
    override fun push(queueName: String, id: String, priority: Int, offsetTimeInSecond: Long) {}
    override fun push(queueName: String, messages: MutableList<Message>) {}
    override fun pushIfNotExists(queueName: String, id: String, offsetTimeInSecond: Long): Boolean = true
    override fun pushIfNotExists(queueName: String, id: String, priority: Int, offsetTimeInSecond: Long): Boolean = true
    override fun pop(queueName: String, count: Int, timeout: Int): MutableList<String> = mutableListOf()
    override fun pollMessages(queueName: String, count: Int, timeout: Int): MutableList<Message> = mutableListOf()
    override fun remove(queueName: String, messageId: String) {}
    override fun getSize(queueName: String): Int = 0
    override fun ack(queueName: String, messageId: String): Boolean = true
    override fun setUnackTimeout(queueName: String, messageId: String, unackTimeout: Long): Boolean = true
    override fun flush(queueName: String) {}
    override fun queuesDetail(): MutableMap<String, Long> = mutableMapOf()
    override fun queuesDetailVerbose(): MutableMap<String, MutableMap<String, MutableMap<String, Long>>> = mutableMapOf()
    override fun postpone(queueName: String, messageId: String, priority: Int, postponeDurationInSeconds: Long): Boolean = true
    override fun containsMessage(queueName: String, messageId: String): Boolean = false
    override fun resetOffsetTime(queueName: String, messageId: String): Boolean = true
}

private class NoOpIndexDAO : IndexDAO {
    override fun setup() {}
    override fun indexWorkflow(workflow: WorkflowSummary) {}
    override fun asyncIndexWorkflow(workflow: WorkflowSummary): CompletableFuture<Void> = CompletableFuture.completedFuture(null)
    override fun indexTask(task: TaskSummary) {}
    override fun asyncIndexTask(task: TaskSummary): CompletableFuture<Void> = CompletableFuture.completedFuture(null)
    override fun searchWorkflows(query: String, freeText: String, start: Int, count: Int, sort: MutableList<String>?): SearchResult<String> = SearchResult(0, emptyList())
    override fun searchWorkflowSummary(query: String, freeText: String, start: Int, count: Int, sort: MutableList<String>?): SearchResult<WorkflowSummary> = SearchResult(0, emptyList())
    override fun searchTasks(query: String, freeText: String, start: Int, count: Int, sort: MutableList<String>?): SearchResult<String> = SearchResult(0, emptyList())
    override fun searchTaskSummary(query: String, freeText: String, start: Int, count: Int, sort: MutableList<String>?): SearchResult<TaskSummary> = SearchResult(0, emptyList())
    override fun removeWorkflow(workflowId: String) {}
    override fun asyncRemoveWorkflow(workflowId: String): CompletableFuture<Void> = CompletableFuture.completedFuture(null)
    override fun updateWorkflow(workflowInstanceId: String, keys: Array<String>, values: Array<Any>) {}
    override fun asyncUpdateWorkflow(workflowInstanceId: String, keys: Array<String>, values: Array<Any>): CompletableFuture<Void> = CompletableFuture.completedFuture(null)
    override fun removeTask(workflowId: String, taskId: String) {}
    override fun asyncRemoveTask(workflowId: String, taskId: String): CompletableFuture<Void> = CompletableFuture.completedFuture(null)
    override fun updateTask(workflowId: String, taskId: String, keys: Array<String>, values: Array<Any>) {}
    override fun asyncUpdateTask(workflowId: String, taskId: String, keys: Array<String>, values: Array<Any>): CompletableFuture<Void> = CompletableFuture.completedFuture(null)
    override fun get(workflowInstanceId: String, key: String): String? = null
    override fun addTaskExecutionLogs(logs: MutableList<com.netflix.conductor.common.metadata.tasks.TaskExecLog>) {}
    override fun asyncAddTaskExecutionLogs(logs: MutableList<com.netflix.conductor.common.metadata.tasks.TaskExecLog>): CompletableFuture<Void> = CompletableFuture.completedFuture(null)
    override fun getTaskExecutionLogs(taskId: String): MutableList<com.netflix.conductor.common.metadata.tasks.TaskExecLog> = mutableListOf()
    override fun addEventExecution(eventExecution: EventExecution) {}
    override fun asyncAddEventExecution(eventExecution: EventExecution): CompletableFuture<Void> = CompletableFuture.completedFuture(null)
    override fun getEventExecutions(event: String): MutableList<EventExecution> = mutableListOf()
    override fun addMessage(queue: String, msg: Message) {}
    override fun asyncAddMessage(queue: String, msg: Message): CompletableFuture<Void> = CompletableFuture.completedFuture(null)
    override fun getMessages(queue: String): MutableList<Message> = mutableListOf()
    override fun searchArchivableWorkflows(indexName: String, archiveTtlDays: Long): MutableList<String> = mutableListOf()
    override fun getWorkflowCount(query: String, freeText: String): Long = 0
}

private class NoOpRateLimitingDAO : RateLimitingDAO {
    override fun exceedsRateLimitPerFrequency(task: TaskModel, taskDef: com.netflix.conductor.common.metadata.tasks.TaskDef): Boolean = false
}

private class NoOpConcurrentExecutionLimitDAO : ConcurrentExecutionLimitDAO {
    override fun exceedsLimit(task: TaskModel): Boolean = false
}

private class NoOpPollDataDAO : PollDataDAO {
    override fun updateLastPollData(taskDefName: String, domain: String?, workerId: String) {}
    override fun getPollData(taskDefName: String, domain: String?): PollData? = null
    override fun getPollData(taskDefName: String): MutableList<PollData> = mutableListOf()
    override fun getAllPollData(): MutableList<PollData> = mutableListOf()
}

private class NoOpExternalPayloadStorageUtils : ExternalPayloadStorageUtils(null, null, null) {
    override fun downloadPayload(path: String): MutableMap<String, Any> = mutableMapOf()
}
