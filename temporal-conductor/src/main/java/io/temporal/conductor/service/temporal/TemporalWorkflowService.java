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
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import io.temporal.api.common.v1.WorkflowExecution;
import io.temporal.api.enums.v1.WorkflowExecutionStatus;
import io.temporal.api.workflow.v1.WorkflowExecutionInfo;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest;
import io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse;
import io.temporal.api.workflowservice.v1.ListWorkflowExecutionsRequest;
import io.temporal.api.workflowservice.v1.ListWorkflowExecutionsResponse;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.conductor.dto.SearchResult;
import io.temporal.conductor.dto.StartWorkflowRequest;
import io.temporal.conductor.dto.Workflow;
import io.temporal.conductor.dto.WorkflowStatus;
import io.temporal.conductor.dto.WorkflowSummary;
import io.temporal.conductor.service.MetadataService;
import io.temporal.conductor.service.WorkflowService;
import io.temporal.conductor.workflow.model.ConductorWorkflowInput;
import io.temporal.conductor.workflow.model.TaskState;
import io.temporal.conductor.workflow.model.WorkflowState;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

/**
 * Temporal-backed implementation of WorkflowService.
 *
 * <p>This service uses Temporal's WorkflowClient to start, query, and manage
 * Conductor workflows running on the Temporal backend.
 */
@Service
@Profile("temporal")
public class TemporalWorkflowService implements WorkflowService {

    private static final Logger logger = LoggerFactory.getLogger(TemporalWorkflowService.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final WorkflowClient workflowClient;
    private final MetadataService metadataService;
    private final String taskQueue;
    private final String namespace;

    /**
     * Creates a new TemporalWorkflowService.
     *
     * @param workflowClient the Temporal workflow client
     * @param metadataService the metadata service for workflow/task definitions
     * @param taskQueue the task queue for workflows
     * @param namespace the Temporal namespace
     */
    public TemporalWorkflowService(
            WorkflowClient workflowClient,
            MetadataService metadataService,
            @Value("${temporal.task-queue:conductor-workflows}") String taskQueue,
            @Value("${temporal.namespace:conductor}") String namespace) {
        this.workflowClient = workflowClient;
        this.metadataService = metadataService;
        this.taskQueue = taskQueue;
        this.namespace = namespace;
    }

    @Override
    public String startWorkflow(StartWorkflowRequest request) {
        String workflowId = UUID.randomUUID().toString();
        logger.info("Starting workflow: name={}, id={}", request.getName(), workflowId);

        try {
            WorkflowDef workflowDef;
            if (request.getWorkflowDef() != null) {
                workflowDef = request.getWorkflowDef();
            } else {
                int version = request.getVersion() != null ? request.getVersion() : 0;
                if (version > 0) {
                    workflowDef = metadataService.getWorkflowDef(request.getName(), version);
                } else {
                    workflowDef = metadataService.getLatestWorkflowDef(request.getName());
                }
            }

            if (workflowDef == null) {
                throw new IllegalArgumentException(
                        "Workflow definition not found: " + request.getName());
            }

            Map<String, Object> workflowInput = request.getInput() != null
                    ? request.getInput() : Collections.emptyMap();
            Map<String, String> taskDefsJson = buildTaskDefsJson(workflowDef, workflowInput);
            Map<String, String> workflowDefsJson = buildWorkflowDefsJson(workflowDef);

            ConductorWorkflowInput input = ConductorWorkflowInput.builder()
                    .workflowDefJson(OBJECT_MAPPER.writeValueAsString(workflowDef))
                    .workflowInput(request.getInput() != null
                            ? request.getInput() : Collections.emptyMap())
                    .taskDefsJson(taskDefsJson)
                    .workflowDefsJson(workflowDefsJson)
                    .correlationId(request.getCorrelationId())
                    .priority(request.getPriority())
                    .createdBy(request.getCreatedBy())
                    .build();

            WorkflowOptions options = WorkflowOptions.newBuilder()
                    .setWorkflowId(workflowId)
                    .setTaskQueue(taskQueue)
                    .build();

            WorkflowStub workflow = workflowClient.newUntypedWorkflowStub(
                    workflowDef.getName(), options);
            workflow.start(input);

            logger.info("Workflow started successfully: type={}, id={}", workflowDef.getName(), workflowId);
            return workflowId;

        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize workflow definition", e);
        }
    }

    @Override
    public Workflow getWorkflow(String workflowId, boolean includeTasks) {
        logger.debug("Getting workflow: id={}, includeTasks={}", workflowId, includeTasks);

        try {
            WorkflowStub workflowStub = workflowClient.newUntypedWorkflowStub(workflowId);
            WorkflowState state = workflowStub.query("getWorkflow", WorkflowState.class);

            return convertToWorkflow(workflowId, state, includeTasks);
        } catch (Exception e) {
            logger.warn("Failed to query workflow {}: {}", workflowId, e.getMessage());
            // Return basic info from describe if query fails
            return getWorkflowFromDescription(workflowId, includeTasks);
        }
    }

    @Override
    public WorkflowStatus getWorkflowStatus(String workflowId) {
        logger.debug("Getting workflow status: {}", workflowId);

        try {
            WorkflowStub workflowStub = workflowClient.newUntypedWorkflowStub(workflowId);
            WorkflowState state = workflowStub.query("getWorkflow", WorkflowState.class);

            WorkflowStatus status = new WorkflowStatus();
            status.setWorkflowId(workflowId);
            status.setStatus(state.getStatus());
            status.setCorrelationId(state.getCorrelationId());
            status.setOutput(state.getOutput());
            return status;
        } catch (Exception e) {
            logger.warn("Failed to query workflow status {}: {}", workflowId, e.getMessage());
            return getWorkflowStatusFromDescription(workflowId);
        }
    }

    @Override
    public void terminateWorkflow(String workflowId, String reason) {
        logger.info("Terminating workflow: id={}, reason={}", workflowId, reason);
        WorkflowStub workflowStub = workflowClient.newUntypedWorkflowStub(workflowId);
        workflowStub.terminate(reason);
    }

    @Override
    public void pauseWorkflow(String workflowId) {
        logger.info("Pausing workflow: {}", workflowId);
        WorkflowStub workflowStub = workflowClient.newUntypedWorkflowStub(workflowId);
        workflowStub.signal("pause");
    }

    @Override
    public void resumeWorkflow(String workflowId) {
        logger.info("Resuming workflow: {}", workflowId);
        WorkflowStub workflowStub = workflowClient.newUntypedWorkflowStub(workflowId);
        workflowStub.signal("resume");
    }

    @Override
    public String restartWorkflow(String workflowId, boolean useLatestDefinitions) {
        logger.info("Restarting workflow: id={}, useLatest={}", workflowId, useLatestDefinitions);

        Workflow originalWorkflow = getWorkflow(workflowId, false);

        StartWorkflowRequest request = new StartWorkflowRequest();
        request.setName(originalWorkflow.getWorkflowType());
        request.setVersion(useLatestDefinitions ? null : originalWorkflow.getVersion());
        request.setInput(originalWorkflow.getInput());
        request.setCorrelationId(originalWorkflow.getCorrelationId());
        request.setPriority(originalWorkflow.getPriority());

        return startWorkflow(request);
    }

    @Override
    public void retryWorkflow(String workflowId, boolean resumeSubworkflowTasks) {
        logger.info("Retrying workflow: id={}", workflowId);
        restartWorkflow(workflowId, false);
    }

    @Override
    public List<String> getRunningWorkflows(
            String name, Integer version, Long startTime, Long endTime) {
        logger.debug("Getting running workflows: name={}", name);

        List<String> workflowIds = new ArrayList<>();
        try {
            StringBuilder queryBuilder = new StringBuilder();
            queryBuilder.append("ExecutionStatus = 'Running'");

            if (name != null && !name.isEmpty()) {
                queryBuilder.append(" AND WorkflowType = '").append(name).append("'");
            }

            ListWorkflowExecutionsRequest request = ListWorkflowExecutionsRequest.newBuilder()
                    .setNamespace(namespace)
                    .setQuery(queryBuilder.toString())
                    .setPageSize(1000)
                    .build();

            ListWorkflowExecutionsResponse response = workflowClient.getWorkflowServiceStubs()
                    .blockingStub()
                    .listWorkflowExecutions(request);

            for (WorkflowExecutionInfo info : response.getExecutionsList()) {
                workflowIds.add(info.getExecution().getWorkflowId());
            }
        } catch (Exception e) {
            logger.warn("Failed to list running workflows: {}", e.getMessage());
        }

        return workflowIds;
    }

    @Override
    public SearchResult<WorkflowSummary> searchWorkflows(
            int start, int size, String sort, String freeText, String query) {
        logger.debug("Searching workflows: start={}, size={}, query={}", start, size, query);

        try {
            String temporalQuery = buildTemporalQuery(query, freeText);

            ListWorkflowExecutionsRequest request = ListWorkflowExecutionsRequest.newBuilder()
                    .setNamespace(namespace)
                    .setQuery(temporalQuery)
                    .setPageSize(size > 0 ? size : 100)
                    .build();

            ListWorkflowExecutionsResponse response = workflowClient.getWorkflowServiceStubs()
                    .blockingStub()
                    .listWorkflowExecutions(request);

            List<WorkflowSummary> summaries = new ArrayList<>();
            for (WorkflowExecutionInfo info : response.getExecutionsList()) {
                WorkflowSummary summary = convertToWorkflowSummary(info);
                summaries.add(summary);
            }

            return new SearchResult<>(summaries.size(), summaries);
        } catch (Exception e) {
            logger.warn("Failed to search workflows: {}", e.getMessage());
            return new SearchResult<>(0, Collections.emptyList());
        }
    }

    @Override
    public SearchResult<Workflow> searchWorkflowsV2(
            int start, int size, String sort, String freeText, String query) {
        logger.debug("Searching workflows v2: start={}, size={}, query={}", start, size, query);

        try {
            String temporalQuery = buildTemporalQuery(query, freeText);

            ListWorkflowExecutionsRequest request = ListWorkflowExecutionsRequest.newBuilder()
                    .setNamespace(namespace)
                    .setQuery(temporalQuery)
                    .setPageSize(size > 0 ? size : 100)
                    .build();

            ListWorkflowExecutionsResponse response = workflowClient.getWorkflowServiceStubs()
                    .blockingStub()
                    .listWorkflowExecutions(request);

            List<Workflow> workflows = new ArrayList<>();
            for (WorkflowExecutionInfo info : response.getExecutionsList()) {
                try {
                    Workflow workflow = getWorkflow(info.getExecution().getWorkflowId(), true);
                    if (workflow != null) {
                        workflows.add(workflow);
                    }
                } catch (Exception e) {
                    logger.debug("Failed to get workflow details for {}: {}",
                            info.getExecution().getWorkflowId(), e.getMessage());
                }
            }

            return new SearchResult<>(workflows.size(), workflows);
        } catch (Exception e) {
            logger.warn("Failed to search workflows v2: {}", e.getMessage());
            return new SearchResult<>(0, Collections.emptyList());
        }
    }

    private String buildTemporalQuery(String conductorQuery, String freeText) {
        StringBuilder queryBuilder = new StringBuilder();
        boolean hasClause = false;

        if (conductorQuery != null && !conductorQuery.isEmpty() && !conductorQuery.equals("*")) {
            String[] parts = conductorQuery.split("\\s+AND\\s+");
            for (String part : parts) {
                String[] keyValue = part.split(":", 2);
                if (keyValue.length == 2) {
                    String key = keyValue[0].trim();
                    String value = keyValue[1].trim();

                    if (hasClause) {
                        queryBuilder.append(" AND ");
                    }
                    hasClause = true;

                    switch (key.toLowerCase()) {
                        case "status":
                            queryBuilder.append("ConductorStatus = '").append(value).append("'");
                            break;
                        case "workflowtype":
                            queryBuilder.append("WorkflowType = '").append(value).append("'");
                            break;
                        case "workflowid":
                            queryBuilder.append("WorkflowId = '").append(value).append("'");
                            break;
                        default:
                            if (key.startsWith("Conductor") || key.equals("WorkflowId") || key.equals("ExecutionStatus")) {
                                queryBuilder.append(key).append(" = '").append(value).append("'");
                            } else {
                                hasClause = false; // Don't count unknown fields
                            }
                            break;
                    }
                }
            }
        }

        String query = queryBuilder.length() > 0 ? queryBuilder.toString() : "";
        logger.debug("Built Temporal query: {}", query);
        return query;
    }

    private WorkflowSummary convertToWorkflowSummary(WorkflowExecutionInfo info) {
        WorkflowSummary summary = new WorkflowSummary();
        summary.setWorkflowId(info.getExecution().getWorkflowId());
        summary.setWorkflowType(info.getType().getName());
        summary.setStatus(mapTemporalStatusToConductor(info.getStatus()));

        if (info.hasStartTime()) {
            long startMillis = info.getStartTime().getSeconds() * 1000
                    + info.getStartTime().getNanos() / 1_000_000;
            summary.setStartTime(startMillis);
        }
        if (info.hasCloseTime()) {
            long endMillis = info.getCloseTime().getSeconds() * 1000
                    + info.getCloseTime().getNanos() / 1_000_000;
            summary.setEndTime(endMillis);
        }

        return summary;
    }

    private static String mapTemporalStatusToConductor(WorkflowExecutionStatus temporalStatus) {
        switch (temporalStatus) {
            case WORKFLOW_EXECUTION_STATUS_RUNNING:
                return "RUNNING";
            case WORKFLOW_EXECUTION_STATUS_COMPLETED:
                return "COMPLETED";
            case WORKFLOW_EXECUTION_STATUS_FAILED:
                return "FAILED";
            case WORKFLOW_EXECUTION_STATUS_CANCELED:
                return "TERMINATED";
            case WORKFLOW_EXECUTION_STATUS_TERMINATED:
                return "TERMINATED";
            case WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
                return "RUNNING";
            case WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
                return "TIMED_OUT";
            default:
                return "UNKNOWN";
        }
    }

    private Map<String, String> buildTaskDefsJson(WorkflowDef workflowDef,
            Map<String, Object> workflowInput) {
        Map<String, String> taskDefsJson = new HashMap<>();
        java.util.Set<String> visitedWorkflows = new HashSet<>();

        // Collect task names from main workflow and all sub-workflows
        java.util.Set<String> allTaskNames = new HashSet<>();
        collectAllTaskNames(workflowDef, allTaskNames, visitedWorkflows);

        // Also collect task names from workflow input (for FORK_JOIN_DYNAMIC with dynamicForkTasksParam)
        collectTaskNamesFromWorkflowInput(workflowInput, allTaskNames);

        allTaskNames.forEach(taskName -> {
            TaskDef taskDef = metadataService.getTaskDef(taskName);
            if (taskDef != null) {
                try {
                    taskDefsJson.put(taskName, OBJECT_MAPPER.writeValueAsString(taskDef));
                } catch (JsonProcessingException e) {
                    logger.warn("Failed to serialize task definition: {}", taskName);
                }
            } else {
                TaskDef minimalDef = new TaskDef();
                minimalDef.setName(taskName);
                try {
                    taskDefsJson.put(taskName, OBJECT_MAPPER.writeValueAsString(minimalDef));
                } catch (JsonProcessingException e) {
                    logger.warn("Failed to serialize minimal task definition: {}", taskName);
                }
            }
        });

        return taskDefsJson;
    }

    /**
     * Collect all task names from a workflow and its sub-workflows recursively.
     */
    private void collectAllTaskNames(WorkflowDef workflowDef, java.util.Set<String> taskNames,
            java.util.Set<String> visitedWorkflows) {
        String key = workflowDef.getName() + ":" + workflowDef.getVersion();
        if (visitedWorkflows.contains(key)) {
            return;
        }
        visitedWorkflows.add(key);

        // Collect task names from this workflow
        taskNames.addAll(collectTaskNames(workflowDef.getTasks()));

        // Recursively collect from sub-workflows
        collectTaskNamesFromSubWorkflows(workflowDef.getTasks(), taskNames, visitedWorkflows);
    }

    /**
     * Recursively traverse tasks to find SUB_WORKFLOW tasks and collect their task names.
     */
    private void collectTaskNamesFromSubWorkflows(List<WorkflowTask> tasks,
            java.util.Set<String> taskNames, java.util.Set<String> visitedWorkflows) {
        if (tasks == null) {
            return;
        }

        for (WorkflowTask task : tasks) {
            if ("SUB_WORKFLOW".equals(task.getType())) {
                com.netflix.conductor.common.metadata.workflow.SubWorkflowParams subParams =
                        task.getSubWorkflowParam();
                if (subParams != null) {
                    WorkflowDef subDef = resolveSubWorkflowDef(subParams);
                    if (subDef != null) {
                        collectAllTaskNames(subDef, taskNames, visitedWorkflows);
                    }
                }
            }

            // Recurse into nested structures
            if (task.getDecisionCases() != null) {
                for (List<WorkflowTask> caseTasks : task.getDecisionCases().values()) {
                    collectTaskNamesFromSubWorkflows(caseTasks, taskNames, visitedWorkflows);
                }
            }
            collectTaskNamesFromSubWorkflows(task.getDefaultCase(), taskNames, visitedWorkflows);

            if (task.getForkTasks() != null) {
                for (List<WorkflowTask> forkBranch : task.getForkTasks()) {
                    collectTaskNamesFromSubWorkflows(forkBranch, taskNames, visitedWorkflows);
                }
            }

            collectTaskNamesFromSubWorkflows(task.getLoopOver(), taskNames, visitedWorkflows);
        }
    }

    /**
     * Resolve a sub-workflow definition from SubWorkflowParams.
     */
    private WorkflowDef resolveSubWorkflowDef(
            com.netflix.conductor.common.metadata.workflow.SubWorkflowParams subParams) {
        Object workflowDefObj = subParams.getWorkflowDefinition();
        if (workflowDefObj != null) {
            if (workflowDefObj instanceof WorkflowDef) {
                return (WorkflowDef) workflowDefObj;
            } else {
                try {
                    String json = OBJECT_MAPPER.writeValueAsString(workflowDefObj);
                    return OBJECT_MAPPER.readValue(json, WorkflowDef.class);
                } catch (JsonProcessingException e) {
                    logger.warn("Failed to convert inline workflow definition", e);
                    return null;
                }
            }
        } else if (subParams.getName() != null) {
            String name = subParams.getName();
            Integer version = subParams.getVersion();
            return version != null && version > 0
                    ? metadataService.getWorkflowDef(name, version)
                    : metadataService.getLatestWorkflowDef(name);
        }
        return null;
    }

    private List<String> collectTaskNames(
            List<com.netflix.conductor.common.metadata.workflow.WorkflowTask> tasks) {
        List<String> names = new ArrayList<>();
        if (tasks == null) {
            return names;
        }

        for (com.netflix.conductor.common.metadata.workflow.WorkflowTask task : tasks) {
            if (task.getName() != null) {
                names.add(task.getName());
            }

            // Handle FORK_JOIN_DYNAMIC: extract forkTaskName from input parameters
            if (TaskType.FORK_JOIN_DYNAMIC.name().equals(task.getType())) {
                Map<String, Object> inputParams = task.getInputParameters();
                if (inputParams != null) {
                    Object forkTaskName = inputParams.get("forkTaskName");
                    if (forkTaskName instanceof String) {
                        names.add((String) forkTaskName);
                    }
                }
            }

            if (task.getDecisionCases() != null) {
                for (List<com.netflix.conductor.common.metadata.workflow.WorkflowTask> caseTasks
                        : task.getDecisionCases().values()) {
                    names.addAll(collectTaskNames(caseTasks));
                }
            }
            names.addAll(collectTaskNames(task.getDefaultCase()));

            if (task.getForkTasks() != null) {
                for (List<com.netflix.conductor.common.metadata.workflow.WorkflowTask> forkBranch
                        : task.getForkTasks()) {
                    names.addAll(collectTaskNames(forkBranch));
                }
            }

            names.addAll(collectTaskNames(task.getLoopOver()));
        }

        return names;
    }

    /**
     * Collect task names from workflow input for FORK_JOIN_DYNAMIC with dynamicForkTasksParam.
     * This handles the case where task definitions are passed in the workflow input at runtime.
     */
    @SuppressWarnings("unchecked")
    private void collectTaskNamesFromWorkflowInput(Map<String, Object> workflowInput,
            java.util.Set<String> taskNames) {
        if (workflowInput == null) {
            return;
        }

        // Look for dynamicTasks array in workflow input
        // This is used by FORK_JOIN_DYNAMIC with dynamicForkTasksParam
        for (Object value : workflowInput.values()) {
            if (value instanceof List) {
                List<?> list = (List<?>) value;
                for (Object item : list) {
                    if (item instanceof Map) {
                        Map<String, Object> taskDef = (Map<String, Object>) item;
                        // Check if this looks like a task definition (has name and type)
                        Object name = taskDef.get("name");
                        Object type = taskDef.get("type");
                        if (name instanceof String && type != null) {
                            taskNames.add((String) name);
                        }
                    }
                }
            }
        }
    }

    /**
     * Recursively collect all sub-workflow definitions referenced by the workflow.
     * Returns a map of "name:version" -> workflow definition JSON.
     */
    private Map<String, String> buildWorkflowDefsJson(WorkflowDef workflowDef) {
        Map<String, String> workflowDefsJson = new HashMap<>();
        java.util.Set<String> visited = new HashSet<>();

        // Recursively collect sub-workflow definitions
        collectSubWorkflowDefs(workflowDef.getTasks(), workflowDefsJson, visited);

        return workflowDefsJson;
    }

    private void collectSubWorkflowDefs(
            List<WorkflowTask> tasks,
            Map<String, String> workflowDefsJson,
            java.util.Set<String> visited) {

        if (tasks == null) {
            return;
        }

        for (WorkflowTask task : tasks) {
            // Handle SUB_WORKFLOW tasks
            if ("SUB_WORKFLOW".equals(task.getType())) {
                com.netflix.conductor.common.metadata.workflow.SubWorkflowParams subParams =
                        task.getSubWorkflowParam();

                if (subParams != null) {
                    // Check if inline definition is provided
                    Object workflowDefObj = subParams.getWorkflowDefinition();
                    if (workflowDefObj != null) {
                        // Inline definition - convert and add it
                        WorkflowDef inlineDef;
                        if (workflowDefObj instanceof WorkflowDef) {
                            inlineDef = (WorkflowDef) workflowDefObj;
                        } else {
                            // Convert from Map to WorkflowDef
                            try {
                                String json = OBJECT_MAPPER.writeValueAsString(workflowDefObj);
                                inlineDef = OBJECT_MAPPER.readValue(json, WorkflowDef.class);
                            } catch (JsonProcessingException e) {
                                logger.warn("Failed to convert inline workflow definition", e);
                                continue;
                            }
                        }
                        String key = inlineDef.getName() + ":" + inlineDef.getVersion();
                        if (!visited.contains(key)) {
                            visited.add(key);
                            try {
                                workflowDefsJson.put(key, OBJECT_MAPPER.writeValueAsString(inlineDef));
                            } catch (JsonProcessingException e) {
                                logger.warn("Failed to serialize inline workflow definition: {}", key);
                            }
                            // Recurse into inline workflow
                            collectSubWorkflowDefs(inlineDef.getTasks(), workflowDefsJson, visited);
                        }
                    } else if (subParams.getName() != null) {
                        // Referenced workflow - look it up
                        String name = subParams.getName();
                        Integer version = subParams.getVersion();
                        String key = name + ":" + (version != null ? version : 0);

                        if (!visited.contains(key)) {
                            visited.add(key);
                            WorkflowDef subDef = version != null && version > 0
                                    ? metadataService.getWorkflowDef(name, version)
                                    : metadataService.getLatestWorkflowDef(name);

                            if (subDef != null) {
                                String actualKey = subDef.getName() + ":" + subDef.getVersion();
                                try {
                                    workflowDefsJson.put(actualKey, OBJECT_MAPPER.writeValueAsString(subDef));
                                } catch (JsonProcessingException e) {
                                    logger.warn("Failed to serialize sub-workflow definition: {}", actualKey);
                                }
                                // Recurse into referenced sub-workflow
                                collectSubWorkflowDefs(subDef.getTasks(), workflowDefsJson, visited);
                            } else {
                                logger.warn("Sub-workflow definition not found: {} version {}",
                                        name, version);
                            }
                        }
                    }
                }
            }

            // Recurse into nested structures
            if (task.getDecisionCases() != null) {
                for (List<WorkflowTask> caseTasks : task.getDecisionCases().values()) {
                    collectSubWorkflowDefs(caseTasks, workflowDefsJson, visited);
                }
            }
            collectSubWorkflowDefs(task.getDefaultCase(), workflowDefsJson, visited);

            if (task.getForkTasks() != null) {
                for (List<WorkflowTask> forkBranch : task.getForkTasks()) {
                    collectSubWorkflowDefs(forkBranch, workflowDefsJson, visited);
                }
            }

            collectSubWorkflowDefs(task.getLoopOver(), workflowDefsJson, visited);
        }
    }

    private Workflow convertToWorkflow(
            String workflowId, WorkflowState state, boolean includeTasks) {
        Workflow workflow = new Workflow();
        workflow.setWorkflowId(workflowId);
        workflow.setWorkflowType(state.getWorkflowType());
        workflow.setVersion(state.getVersion());
        workflow.setStatus(state.getStatus());
        workflow.setCorrelationId(state.getCorrelationId());
        workflow.setPriority(state.getPriority());
        workflow.setOwnerApp(state.getOwnerApp());
        workflow.setCreatedBy(state.getCreatedBy());
        workflow.setInput(state.getInput());
        workflow.setOutput(state.getOutput());
        workflow.setVariables(state.getVariables());
        workflow.setReasonForIncompletion(state.getReasonForIncompletion());

        WorkflowDef workflowDef = metadataService.getWorkflowDef(
                state.getWorkflowType(), state.getVersion());
        if (workflowDef == null) {
            workflowDef = metadataService.getLatestWorkflowDef(state.getWorkflowType());
        }
        workflow.setWorkflowDefinition(workflowDef);

        if (state.getCreateTime() > 0) {
            workflow.setCreateTime(state.getCreateTime());
        }
        if (state.getUpdateTime() > 0) {
            workflow.setUpdateTime(state.getUpdateTime());
        }
        if (state.getEndTime() != null && state.getEndTime() > 0) {
            workflow.setEndTime(state.getEndTime());
        }

        if (includeTasks && state.getTasks() != null) {
            List<Task> tasks = new ArrayList<>();
            for (TaskState taskState : state.getTasks()) {
                tasks.add(convertToTask(taskState, workflowId));
            }
            workflow.setTasks(tasks);

            HashSet<String> failedTaskNames = new HashSet<>();
            for (TaskState taskState : state.getTasks()) {
                if ("FAILED".equals(taskState.getStatus())
                        || "FAILED_WITH_TERMINAL_ERROR".equals(taskState.getStatus())) {
                    failedTaskNames.add(taskState.getReferenceTaskName());
                }
            }
            workflow.setFailedReferenceTaskNames(failedTaskNames);
        } else {
            workflow.setTasks(Collections.emptyList());
        }

        return workflow;
    }

    private Task convertToTask(TaskState taskState, String workflowId) {
        Task task = new Task();
        task.setTaskId(taskState.getTaskId());
        task.setTaskType(taskState.getTaskType());
        task.setTaskDefName(taskState.getTaskDefName());
        task.setReferenceTaskName(taskState.getReferenceTaskName());
        task.setWorkflowInstanceId(workflowId);
        task.setStatus(Task.Status.valueOf(taskState.getStatus()));
        task.setRetryCount(taskState.getRetryCount());
        task.setScheduledTime(taskState.getScheduledTime());
        task.setStartTime(taskState.getStartTime());
        // Only set updateTime and endTime if they have valid values (> 0)
        // Conductor UI can't parse 0 as a timestamp ("Invalid time value" error)
        if (taskState.getUpdateTime() > 0) {
            task.setUpdateTime(taskState.getUpdateTime());
        }
        if (taskState.getEndTime() > 0) {
            task.setEndTime(taskState.getEndTime());
        }
        task.setInputData(taskState.getInputData());
        task.setOutputData(taskState.getOutputData());
        task.setWorkerId(taskState.getWorkerId());
        task.setPollCount(taskState.getPollCount());
        task.setIteration(taskState.getIteration());
        task.setReasonForIncompletion(taskState.getReasonForIncompletion());

        // Set workflowTask - the UI needs this for task type information
        WorkflowTask workflowTask = new WorkflowTask();
        workflowTask.setName(taskState.getTaskDefName());
        workflowTask.setTaskReferenceName(taskState.getReferenceTaskName());
        workflowTask.setType(taskState.getTaskType());
        task.setWorkflowTask(workflowTask);

        return task;
    }

    private Workflow getWorkflowFromDescription(String workflowId, boolean includeTasks) {
        try {
            DescribeWorkflowExecutionRequest request = DescribeWorkflowExecutionRequest.newBuilder()
                    .setNamespace(namespace)
                    .setExecution(WorkflowExecution.newBuilder()
                            .setWorkflowId(workflowId)
                            .build())
                    .build();

            DescribeWorkflowExecutionResponse response = workflowClient.getWorkflowServiceStubs()
                    .blockingStub()
                    .describeWorkflowExecution(request);

            Workflow workflow = new Workflow();
            workflow.setWorkflowId(workflowId);
            workflow.setStatus(mapTemporalStatus(
                    response.getWorkflowExecutionInfo().getStatus()));
            workflow.setTasks(Collections.emptyList());
            return workflow;
        } catch (Exception e) {
            logger.error("Failed to describe workflow {}: {}", workflowId, e.getMessage());
            Workflow workflow = new Workflow();
            workflow.setWorkflowId(workflowId);
            workflow.setStatus("UNKNOWN");
            workflow.setTasks(Collections.emptyList());
            return workflow;
        }
    }

    private WorkflowStatus getWorkflowStatusFromDescription(String workflowId) {
        try {
            DescribeWorkflowExecutionRequest request = DescribeWorkflowExecutionRequest.newBuilder()
                    .setNamespace(namespace)
                    .setExecution(WorkflowExecution.newBuilder()
                            .setWorkflowId(workflowId)
                            .build())
                    .build();

            DescribeWorkflowExecutionResponse response = workflowClient.getWorkflowServiceStubs()
                    .blockingStub()
                    .describeWorkflowExecution(request);

            WorkflowStatus status = new WorkflowStatus();
            status.setWorkflowId(workflowId);
            status.setStatus(mapTemporalStatus(
                    response.getWorkflowExecutionInfo().getStatus()));
            return status;
        } catch (Exception e) {
            logger.error("Failed to describe workflow {}: {}", workflowId, e.getMessage());
            WorkflowStatus status = new WorkflowStatus();
            status.setWorkflowId(workflowId);
            status.setStatus("UNKNOWN");
            return status;
        }
    }

    private String mapTemporalStatus(WorkflowExecutionStatus temporalStatus) {
        return switch (temporalStatus) {
            case WORKFLOW_EXECUTION_STATUS_RUNNING -> "RUNNING";
            case WORKFLOW_EXECUTION_STATUS_COMPLETED -> "COMPLETED";
            case WORKFLOW_EXECUTION_STATUS_FAILED -> "FAILED";
            case WORKFLOW_EXECUTION_STATUS_CANCELED -> "TERMINATED";
            case WORKFLOW_EXECUTION_STATUS_TERMINATED -> "TERMINATED";
            case WORKFLOW_EXECUTION_STATUS_TIMED_OUT -> "TIMED_OUT";
            default -> "UNKNOWN";
        };
    }
}
