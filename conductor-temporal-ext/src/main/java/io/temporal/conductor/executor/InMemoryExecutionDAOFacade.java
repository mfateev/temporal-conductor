/*
 * Copyright Temporal Technologies Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.temporal.conductor.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.events.EventExecution;
import com.netflix.conductor.common.metadata.tasks.PollData;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskExecLog;
import com.netflix.conductor.common.run.SearchResult;
import com.netflix.conductor.common.run.TaskSummary;
import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.dal.ExecutionDAOFacade;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.storage.DummyPayloadStorage;
import com.netflix.conductor.core.utils.ExternalPayloadStorageUtils;
import com.netflix.conductor.dao.ConcurrentExecutionLimitDAO;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.IndexDAO;
import com.netflix.conductor.dao.PollDataDAO;
import com.netflix.conductor.dao.QueueDAO;
import com.netflix.conductor.dao.RateLimitingDAO;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * In-memory ExecutionDAOFacade for use inside Temporal workflows.
 *
 * <p>Temporal workflows have durable memory - any state in the workflow is automatically
 * persisted and replayed. This means we don't need external database persistence.
 *
 * <p>This class provides no-op implementations for methods that would normally
 * persist to a database:
 * <ul>
 *   <li>updateWorkflow(): Used by SetVariable - no-op since Temporal handles persistence</li>
 *   <li>removeTask(): Used by DoWhile (keepLastN cleanup) - no-op since Temporal manages history</li>
 * </ul>
 */
public class InMemoryExecutionDAOFacade extends ExecutionDAOFacade {

    /**
     * Creates a new InMemoryExecutionDAOFacade with default ObjectMapper.
     */
    public InMemoryExecutionDAOFacade() {
        this(new ObjectMapper());
    }

    /**
     * Creates a new InMemoryExecutionDAOFacade with the specified ObjectMapper.
     *
     * @param objectMapper the ObjectMapper to use for JSON serialization
     */
    public InMemoryExecutionDAOFacade(ObjectMapper objectMapper) {
        super(
                new NoOpExecutionDao(),
                new NoOpQueueDao(),
                new NoOpIndexDao(),
                new NoOpRateLimitingDao(),
                new NoOpConcurrentExecutionLimitDao(),
                new NoOpPollDataDao(),
                objectMapper,
                new ConductorProperties(),
                new NoOpExternalPayloadStorageUtils()
        );
    }

    /**
     * No-op: Temporal automatically persists workflow state.
     * Called by SetVariable after updating workflow variables.
     */
    @Override
    public String updateWorkflow(WorkflowModel workflowModel) {
        // No-op - Temporal handles persistence automatically
        return workflowModel.getWorkflowId();
    }

    /**
     * No-op: Temporal manages history size differently.
     * Called by DoWhile for keepLastN cleanup of old iterations.
     */
    @Override
    public void removeTask(String taskId) {
        // No-op - Temporal handles history management
    }

    // Stub implementations of required DAOs

    private static class NoOpExecutionDao implements ExecutionDAO {
        @Override
        public List<WorkflowModel> getPendingWorkflowsByType(String workflowName, int version) {
            return Collections.emptyList();
        }

        @Override
        public List<WorkflowModel> getWorkflowsByType(
                String workflowName, Long startTime, Long endTime) {
            return Collections.emptyList();
        }

        @Override
        public List<WorkflowModel> getWorkflowsByCorrelationId(
                String workflowName, String correlationId, boolean includeTasks) {
            return Collections.emptyList();
        }

        @Override
        public WorkflowModel getWorkflow(String workflowId) {
            return null;
        }

        @Override
        public WorkflowModel getWorkflow(String workflowId, boolean includeTasks) {
            return null;
        }

        @Override
        public List<String> getRunningWorkflowIds(String workflowName, int version) {
            return Collections.emptyList();
        }

        @Override
        public long getPendingWorkflowCount(String workflowName) {
            return 0;
        }

        @Override
        public String createWorkflow(WorkflowModel workflow) {
            return workflow.getWorkflowId();
        }

        @Override
        public String updateWorkflow(WorkflowModel workflow) {
            return workflow.getWorkflowId();
        }

        @Override
        public boolean removeWorkflow(String workflowId) {
            return true;
        }

        @Override
        public boolean removeWorkflowWithExpiry(String workflowId, int ttlSeconds) {
            return true;
        }

        @Override
        public void removeFromPendingWorkflow(String workflowType, String workflowId) {
        }

        @Override
        public TaskModel getTask(String taskId) {
            return null;
        }

        @Override
        public List<TaskModel> getTasks(String taskType, String startKey, int count) {
            return Collections.emptyList();
        }

        @Override
        public List<TaskModel> getTasks(List<String> taskIds) {
            return Collections.emptyList();
        }

        @Override
        public List<TaskModel> createTasks(List<TaskModel> tasks) {
            return tasks;
        }

        @Override
        public void updateTask(TaskModel task) {
        }

        @Override
        public List<TaskModel> getTasksForWorkflow(String workflowId) {
            return Collections.emptyList();
        }

        @Override
        public boolean removeTask(String taskId) {
            return true;
        }

        @Override
        public List<TaskModel> getPendingTasksForTaskType(String taskType) {
            return Collections.emptyList();
        }

        @Override
        public long getInProgressTaskCount(String taskDefName) {
            return 0;
        }

        @Override
        public boolean addEventExecution(EventExecution eventExecution) {
            return true;
        }

        @Override
        public void updateEventExecution(EventExecution eventExecution) {
        }

        @Override
        public void removeEventExecution(EventExecution eventExecution) {
        }

        @Override
        public boolean canSearchAcrossWorkflows() {
            return false;
        }

        @Override
        public List<TaskModel> getPendingTasksByWorkflow(String workflowId, String taskName) {
            return Collections.emptyList();
        }
    }

    private static class NoOpQueueDao implements QueueDAO {
        @Override
        public void push(String queueName, String id, long offsetTimeInSecond) {
        }

        @Override
        public void push(String queueName, String id, int priority, long offsetTimeInSecond) {
        }

        @Override
        public void push(String queueName, List<Message> messages) {
        }

        @Override
        public boolean pushIfNotExists(String queueName, String id, long offsetTimeInSecond) {
            return true;
        }

        @Override
        public boolean pushIfNotExists(
                String queueName, String id, int priority, long offsetTimeInSecond) {
            return true;
        }

        @Override
        public List<String> pop(String queueName, int count, int timeout) {
            return Collections.emptyList();
        }

        @Override
        public List<Message> pollMessages(String queueName, int count, int timeout) {
            return Collections.emptyList();
        }

        @Override
        public void remove(String queueName, String messageId) {
        }

        @Override
        public int getSize(String queueName) {
            return 0;
        }

        @Override
        public boolean ack(String queueName, String messageId) {
            return true;
        }

        @Override
        public boolean setUnackTimeout(String queueName, String messageId, long unackTimeout) {
            return true;
        }

        @Override
        public void flush(String queueName) {
        }

        @Override
        public Map<String, Long> queuesDetail() {
            return Collections.emptyMap();
        }

        @Override
        public Map<String, Map<String, Map<String, Long>>> queuesDetailVerbose() {
            return Collections.emptyMap();
        }

        @Override
        public boolean postpone(
                String queueName, String messageId, int priority, long postponeDurationInSeconds) {
            return true;
        }

        @Override
        public boolean containsMessage(String queueName, String messageId) {
            return false;
        }

        @Override
        public boolean resetOffsetTime(String queueName, String messageId) {
            return true;
        }
    }

    private static class NoOpIndexDao implements IndexDAO {
        @Override
        public void setup() {
        }

        @Override
        public void indexWorkflow(WorkflowSummary workflow) {
        }

        @Override
        public CompletableFuture<Void> asyncIndexWorkflow(WorkflowSummary workflow) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void indexTask(TaskSummary task) {
        }

        @Override
        public CompletableFuture<Void> asyncIndexTask(TaskSummary task) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public SearchResult<String> searchWorkflows(
                String query, String freeText, int start, int count, List<String> sort) {
            return new SearchResult<>(0, Collections.emptyList());
        }

        @Override
        public SearchResult<WorkflowSummary> searchWorkflowSummary(
                String query, String freeText, int start, int count, List<String> sort) {
            return new SearchResult<>(0, Collections.emptyList());
        }

        @Override
        public SearchResult<String> searchTasks(
                String query, String freeText, int start, int count, List<String> sort) {
            return new SearchResult<>(0, Collections.emptyList());
        }

        @Override
        public SearchResult<TaskSummary> searchTaskSummary(
                String query, String freeText, int start, int count, List<String> sort) {
            return new SearchResult<>(0, Collections.emptyList());
        }

        @Override
        public void removeWorkflow(String workflowId) {
        }

        @Override
        public CompletableFuture<Void> asyncRemoveWorkflow(String workflowId) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void updateWorkflow(String workflowInstanceId, String[] keys, Object[] values) {
        }

        @Override
        public CompletableFuture<Void> asyncUpdateWorkflow(
                String workflowInstanceId, String[] keys, Object[] values) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void removeTask(String workflowId, String taskId) {
        }

        @Override
        public CompletableFuture<Void> asyncRemoveTask(String workflowId, String taskId) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void updateTask(String workflowId, String taskId, String[] keys, Object[] values) {
        }

        @Override
        public CompletableFuture<Void> asyncUpdateTask(
                String workflowId, String taskId, String[] keys, Object[] values) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public String get(String workflowInstanceId, String key) {
            return null;
        }

        @Override
        public void addTaskExecutionLogs(List<TaskExecLog> logs) {
        }

        @Override
        public CompletableFuture<Void> asyncAddTaskExecutionLogs(List<TaskExecLog> logs) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public List<TaskExecLog> getTaskExecutionLogs(String taskId) {
            return Collections.emptyList();
        }

        @Override
        public void addEventExecution(EventExecution eventExecution) {
        }

        @Override
        public CompletableFuture<Void> asyncAddEventExecution(EventExecution eventExecution) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public List<EventExecution> getEventExecutions(String event) {
            return Collections.emptyList();
        }

        @Override
        public void addMessage(String queue, Message msg) {
        }

        @Override
        public CompletableFuture<Void> asyncAddMessage(String queue, Message msg) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public List<Message> getMessages(String queue) {
            return Collections.emptyList();
        }

        @Override
        public List<String> searchArchivableWorkflows(String indexName, long archiveTtlDays) {
            return Collections.emptyList();
        }

        @Override
        public long getWorkflowCount(String query, String freeText) {
            return 0;
        }
    }

    private static class NoOpRateLimitingDao implements RateLimitingDAO {
        @Override
        public boolean exceedsRateLimitPerFrequency(TaskModel task, TaskDef taskDef) {
            return false;
        }
    }

    private static class NoOpConcurrentExecutionLimitDao implements ConcurrentExecutionLimitDAO {
        @Override
        public boolean exceedsLimit(TaskModel task) {
            return false;
        }
    }

    private static class NoOpPollDataDao implements PollDataDAO {
        @Override
        public void updateLastPollData(String taskDefName, String domain, String workerId) {
        }

        @Override
        public PollData getPollData(String taskDefName, String domain) {
            return null;
        }

        @Override
        public List<PollData> getPollData(String taskDefName) {
            return Collections.emptyList();
        }

        @Override
        public List<PollData> getAllPollData() {
            return Collections.emptyList();
        }
    }

    private static class NoOpExternalPayloadStorageUtils extends ExternalPayloadStorageUtils {
        NoOpExternalPayloadStorageUtils() {
            super(new DummyPayloadStorage(), new ConductorProperties(), new ObjectMapper());
        }

        @Override
        public Map<String, Object> downloadPayload(String path) {
            return new HashMap<>();
        }
    }
}
