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

package io.temporal.conductor.service.temporal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.WorkflowExecutionStatus;
import io.temporal.api.workflow.v1.WorkflowExecutionInfo;
import io.temporal.api.workflowservice.v1.ListWorkflowExecutionsRequest;
import io.temporal.api.workflowservice.v1.ListWorkflowExecutionsResponse;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowExecutionAlreadyStarted;
import io.temporal.client.WorkflowNotFoundException;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.conductor.service.MetadataService;
import io.temporal.conductor.workflow.DefinitionSearchAttributes;
import io.temporal.conductor.workflow.DefinitionWorkflow;
import io.temporal.common.SearchAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

/**
 * Temporal-backed implementation of MetadataService.
 *
 * <p>This service stores workflow and task definitions as long-running Temporal workflows. Each
 * definition is a DefinitionWorkflow that holds state forever using {@code Workflow.await(() ->
 * false)}.
 *
 * <p>A Caffeine cache is used for read performance with a 60-second TTL.
 */
@Service
@Profile("temporal")
public class TemporalMetadataService implements MetadataService {

    private static final Logger logger = LoggerFactory.getLogger(TemporalMetadataService.class);

    private final WorkflowClient workflowClient;
    private final ObjectMapper objectMapper;
    private final String taskQueue;
    private final String namespace;

    // Cache for workflow definitions: key = "workflow:{name}:{version}"
    private final Cache<String, WorkflowDef> workflowDefCache;

    // Cache for task definitions: key = task type name
    private final Cache<String, TaskDef> taskDefCache;

    public TemporalMetadataService(
            WorkflowClient workflowClient,
            ObjectMapper objectMapper,
            @Value("${temporal.task-queue:conductor-workflows}") String taskQueue,
            @Value("${temporal.namespace:conductor}") String namespace) {
        this.workflowClient = workflowClient;
        this.objectMapper = objectMapper;
        this.taskQueue = taskQueue;
        this.namespace = namespace;

        // Initialize caches with 60-second TTL
        this.workflowDefCache =
                Caffeine.newBuilder().expireAfterWrite(60, TimeUnit.SECONDS).maximumSize(1000)
                        .build();
        this.taskDefCache = Caffeine.newBuilder().expireAfterWrite(60, TimeUnit.SECONDS)
                .maximumSize(1000).build();
    }

    @Override
    public List<WorkflowDef> getAllWorkflowDefs() {
        logger.debug("Getting all workflow definitions via visibility API");

        List<WorkflowDef> allDefs = new ArrayList<>();

        try {
            String query = String.format("WorkflowType = 'DefinitionWorkflow' AND %s = 'workflow'",
                    DefinitionSearchAttributes.DEFINITION_TYPE.getName());

            String nextPageToken = null;
            do {
                ListWorkflowExecutionsRequest.Builder requestBuilder =
                        ListWorkflowExecutionsRequest.newBuilder().setNamespace(namespace)
                                .setQuery(query).setPageSize(100);

                if (nextPageToken != null) {
                    requestBuilder.setNextPageToken(com.google.protobuf.ByteString
                            .copyFromUtf8(nextPageToken));
                }

                ListWorkflowExecutionsResponse response = workflowClient.getWorkflowServiceStubs()
                        .blockingStub().listWorkflowExecutions(requestBuilder.build());

                for (WorkflowExecutionInfo info : response.getExecutionsList()) {
                    if (info.getStatus()
                            == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING) {
                        try {
                            WorkflowDef def = getWorkflowDefFromExecution(info.getExecution());
                            if (def != null) {
                                allDefs.add(def);
                            }
                        } catch (Exception e) {
                            logger.warn("Failed to retrieve workflow definition from {}: {}",
                                    info.getExecution().getWorkflowId(), e.getMessage());
                        }
                    }
                }

                nextPageToken = response.getNextPageToken().isEmpty() ? null
                        : response.getNextPageToken().toStringUtf8();
            } while (nextPageToken != null);
        } catch (io.grpc.StatusRuntimeException e) {
            logger.warn("Visibility API not available, returning empty list for getAllWorkflowDefs");
            return allDefs;
        }

        logger.debug("Found {} workflow definitions", allDefs.size());
        return allDefs;
    }

    @Override
    public WorkflowDef getWorkflowDef(String name, int version) {
        String cacheKey = "workflow:" + name + ":" + version;
        return workflowDefCache.get(cacheKey, k -> {
            logger.debug("Getting workflow definition: name={}, version={}", name, version);
            String workflowId = DefinitionSearchAttributes.workflowDefId(name, version);

            try {
                WorkflowStub stub = workflowClient.newUntypedWorkflowStub(workflowId);
                String json = stub.query("getDefinitionJson", String.class);
                if (json != null) {
                    return deserializeWorkflowDef(json);
                }
            } catch (WorkflowNotFoundException e) {
                logger.debug("Workflow definition not found: name={}, version={}", name, version);
            } catch (Exception e) {
                logger.warn("Failed to get workflow definition: name={}, version={}, error={}",
                        name, version, e.getMessage());
            }
            return null;
        });
    }

    @Override
    public WorkflowDef getLatestWorkflowDef(String name) {
        logger.debug("Getting latest workflow definition: name={}", name);

        try {
            // Query for all versions of this workflow definition using visibility API
            String query = String.format(
                    "WorkflowType = 'DefinitionWorkflow' AND %s = 'workflow' AND %s = '%s'",
                    DefinitionSearchAttributes.DEFINITION_TYPE.getName(),
                    DefinitionSearchAttributes.DEFINITION_NAME.getName(), name);

            ListWorkflowExecutionsRequest request =
                    ListWorkflowExecutionsRequest.newBuilder().setNamespace(namespace)
                            .setQuery(query).setPageSize(100).build();

            ListWorkflowExecutionsResponse response = workflowClient.getWorkflowServiceStubs()
                    .blockingStub().listWorkflowExecutions(request);

            WorkflowDef latest = null;
            int latestVersion = -1;

            for (WorkflowExecutionInfo info : response.getExecutionsList()) {
                if (info.getStatus()
                        == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING) {
                    try {
                        WorkflowDef def = getWorkflowDefFromExecution(info.getExecution());
                        if (def != null && def.getVersion() > latestVersion) {
                            latest = def;
                            latestVersion = def.getVersion();
                        }
                    } catch (Exception e) {
                        logger.warn("Failed to retrieve workflow definition from {}: {}",
                                info.getExecution().getWorkflowId(), e.getMessage());
                    }
                }
            }

            if (latest != null) {
                return latest;
            }
        } catch (io.grpc.StatusRuntimeException e) {
            // Visibility API not available (e.g., in test environment), fall back to version 1
            logger.debug("Visibility API not available, falling back to version 1 lookup");
            return getWorkflowDef(name, 1);
        }

        logger.warn("Workflow definition not found: name={}", name);
        return null;
    }

    @Override
    public void registerWorkflowDef(WorkflowDef workflowDef) {
        logger.info("Registering workflow definition: name={}, version={}", workflowDef.getName(),
                workflowDef.getVersion());

        String workflowId =
                DefinitionSearchAttributes.workflowDefId(workflowDef.getName(),
                        workflowDef.getVersion());

        WorkflowOptions options = WorkflowOptions.newBuilder().setWorkflowId(workflowId)
                .setTaskQueue(taskQueue).setTypedSearchAttributes(buildSearchAttributes(
                        "workflow", workflowDef.getName(), workflowDef.getVersion()))
                .build();

        try {
            // Start the workflow
            DefinitionWorkflow workflow =
                    workflowClient.newWorkflowStub(DefinitionWorkflow.class, options);
            WorkflowClient.start(workflow::hold);

            // Signal the definition
            String json = serializeWorkflowDef(workflowDef);
            workflow.updateDefinition(json);

            // Update cache
            String cacheKey = "workflow:" + workflowDef.getName() + ":" + workflowDef.getVersion();
            workflowDefCache.put(cacheKey, workflowDef);

            logger.info("Workflow definition registered successfully: {}", workflowId);
        } catch (WorkflowExecutionAlreadyStarted e) {
            // Workflow exists, update it via signal
            logger.info("Workflow definition already exists, updating: {}", workflowId);
            DefinitionWorkflow workflow = workflowClient.newWorkflowStub(DefinitionWorkflow.class,
                    workflowId);
            String json = serializeWorkflowDef(workflowDef);
            workflow.updateDefinition(json);

            // Update cache
            String cacheKey = "workflow:" + workflowDef.getName() + ":" + workflowDef.getVersion();
            workflowDefCache.put(cacheKey, workflowDef);
        }
    }

    @Override
    public void updateWorkflowDefs(List<WorkflowDef> workflowDefsList) {
        logger.info("Updating {} workflow definitions", workflowDefsList.size());
        for (WorkflowDef def : workflowDefsList) {
            registerWorkflowDef(def);
        }
    }

    @Override
    public void deleteWorkflowDef(String name, int version) {
        logger.info("Deleting workflow definition: name={}, version={}", name, version);

        String workflowId = DefinitionSearchAttributes.workflowDefId(name, version);

        try {
            WorkflowStub stub = workflowClient.newUntypedWorkflowStub(workflowId);
            stub.terminate("Definition deleted");

            // Invalidate cache
            String cacheKey = "workflow:" + name + ":" + version;
            workflowDefCache.invalidate(cacheKey);

            logger.info("Workflow definition deleted: {}", workflowId);
        } catch (WorkflowNotFoundException e) {
            logger.debug("Workflow definition not found for deletion: {}", workflowId);
        } catch (Exception e) {
            logger.warn("Failed to delete workflow definition: {}, error={}", workflowId,
                    e.getMessage());
            throw new RuntimeException("Failed to delete workflow definition", e);
        }
    }

    @Override
    public List<TaskDef> getAllTaskDefs() {
        logger.debug("Getting all task definitions via visibility API");

        List<TaskDef> allDefs = new ArrayList<>();

        try {
            String query = String.format("WorkflowType = 'DefinitionWorkflow' AND %s = 'task'",
                    DefinitionSearchAttributes.DEFINITION_TYPE.getName());

            String nextPageToken = null;
            do {
                ListWorkflowExecutionsRequest.Builder requestBuilder =
                        ListWorkflowExecutionsRequest.newBuilder().setNamespace(namespace)
                                .setQuery(query).setPageSize(100);

                if (nextPageToken != null) {
                    requestBuilder.setNextPageToken(com.google.protobuf.ByteString
                            .copyFromUtf8(nextPageToken));
                }

                ListWorkflowExecutionsResponse response = workflowClient.getWorkflowServiceStubs()
                        .blockingStub().listWorkflowExecutions(requestBuilder.build());

                for (WorkflowExecutionInfo info : response.getExecutionsList()) {
                    if (info.getStatus()
                            == WorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING) {
                        try {
                            TaskDef def = getTaskDefFromExecution(info.getExecution());
                            if (def != null) {
                                allDefs.add(def);
                            }
                        } catch (Exception e) {
                            logger.warn("Failed to retrieve task definition from {}: {}",
                                    info.getExecution().getWorkflowId(), e.getMessage());
                        }
                    }
                }

                nextPageToken = response.getNextPageToken().isEmpty() ? null
                        : response.getNextPageToken().toStringUtf8();
            } while (nextPageToken != null);
        } catch (io.grpc.StatusRuntimeException e) {
            logger.warn("Visibility API not available, returning empty list for getAllTaskDefs");
            return allDefs;
        }

        logger.debug("Found {} task definitions", allDefs.size());
        return allDefs;
    }

    @Override
    public TaskDef getTaskDef(String taskType) {
        return taskDefCache.get(taskType, k -> {
            logger.debug("Getting task definition: taskType={}", taskType);
            String workflowId = DefinitionSearchAttributes.taskDefId(taskType);

            try {
                WorkflowStub stub = workflowClient.newUntypedWorkflowStub(workflowId);
                String json = stub.query("getDefinitionJson", String.class);
                if (json != null) {
                    return deserializeTaskDef(json);
                }
            } catch (WorkflowNotFoundException e) {
                logger.debug("Task definition not found: taskType={}", taskType);
            } catch (Exception e) {
                logger.warn("Failed to get task definition: taskType={}, error={}", taskType,
                        e.getMessage());
            }
            return null;
        });
    }

    @Override
    public void registerTaskDefs(List<TaskDef> taskDefsList) {
        logger.info("Registering {} task definitions", taskDefsList.size());
        for (TaskDef def : taskDefsList) {
            registerTaskDef(def);
        }
    }

    private void registerTaskDef(TaskDef taskDef) {
        logger.info("Registering task definition: taskType={}", taskDef.getName());

        String workflowId = DefinitionSearchAttributes.taskDefId(taskDef.getName());

        WorkflowOptions options =
                WorkflowOptions.newBuilder().setWorkflowId(workflowId).setTaskQueue(taskQueue)
                        .setTypedSearchAttributes(buildSearchAttributes("task", taskDef.getName(),
                                null))
                        .build();

        try {
            // Start the workflow
            DefinitionWorkflow workflow =
                    workflowClient.newWorkflowStub(DefinitionWorkflow.class, options);
            WorkflowClient.start(workflow::hold);

            // Signal the definition
            String json = serializeTaskDef(taskDef);
            workflow.updateDefinition(json);

            // Update cache
            taskDefCache.put(taskDef.getName(), taskDef);

            logger.info("Task definition registered successfully: {}", workflowId);
        } catch (WorkflowExecutionAlreadyStarted e) {
            // Workflow exists, update it via signal
            logger.info("Task definition already exists, updating: {}", workflowId);
            DefinitionWorkflow workflow = workflowClient.newWorkflowStub(DefinitionWorkflow.class,
                    workflowId);
            String json = serializeTaskDef(taskDef);
            workflow.updateDefinition(json);

            // Update cache
            taskDefCache.put(taskDef.getName(), taskDef);
        }
    }

    @Override
    public void deleteTaskDef(String taskType) {
        logger.info("Deleting task definition: taskType={}", taskType);

        String workflowId = DefinitionSearchAttributes.taskDefId(taskType);

        try {
            WorkflowStub stub = workflowClient.newUntypedWorkflowStub(workflowId);
            stub.terminate("Definition deleted");

            // Invalidate cache
            taskDefCache.invalidate(taskType);

            logger.info("Task definition deleted: {}", workflowId);
        } catch (WorkflowNotFoundException e) {
            logger.debug("Task definition not found for deletion: {}", workflowId);
        } catch (Exception e) {
            logger.warn("Failed to delete task definition: {}, error={}", workflowId,
                    e.getMessage());
            throw new RuntimeException("Failed to delete task definition", e);
        }
    }

    // Helper methods

    private WorkflowDef getWorkflowDefFromExecution(WorkflowExecution execution) {
        WorkflowStub stub = workflowClient.newUntypedWorkflowStub(execution.getWorkflowId());
        String json = stub.query("getDefinitionJson", String.class);
        return json != null ? deserializeWorkflowDef(json) : null;
    }

    private TaskDef getTaskDefFromExecution(WorkflowExecution execution) {
        WorkflowStub stub = workflowClient.newUntypedWorkflowStub(execution.getWorkflowId());
        String json = stub.query("getDefinitionJson", String.class);
        return json != null ? deserializeTaskDef(json) : null;
    }

    private SearchAttributes buildSearchAttributes(String type, String name, Integer version) {
        // Note: ConductorDefinitionVersion is not set because it may hit the Int search attribute
        // limit in some Temporal setups (e.g., dev PostgreSQL has max 3 Int attributes).
        // The version is already encoded in the workflow ID, so it's not needed for lookups.
        return SearchAttributes.newBuilder()
                .set(DefinitionSearchAttributes.DEFINITION_TYPE, type)
                .set(DefinitionSearchAttributes.DEFINITION_NAME, name)
                .build();
    }

    private String serializeWorkflowDef(WorkflowDef def) {
        try {
            return objectMapper.writeValueAsString(def);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize workflow definition", e);
        }
    }

    private WorkflowDef deserializeWorkflowDef(String json) {
        try {
            return objectMapper.readValue(json, WorkflowDef.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize workflow definition", e);
        }
    }

    private String serializeTaskDef(TaskDef def) {
        try {
            return objectMapper.writeValueAsString(def);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize task definition", e);
        }
    }

    private TaskDef deserializeTaskDef(String json) {
        try {
            return objectMapper.readValue(json, TaskDef.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize task definition", e);
        }
    }
}
