package io.temporal.conductor.service.stub;

import io.temporal.conductor.dto.SearchResult;
import io.temporal.conductor.dto.StartWorkflowRequest;
import io.temporal.conductor.dto.Workflow;
import io.temporal.conductor.dto.WorkflowStatus;
import io.temporal.conductor.dto.WorkflowSummary;
import io.temporal.conductor.service.WorkflowService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

/**
 * Stub implementation of WorkflowService for testing and UI validation.
 */
@Service
@Profile("stub")
public class StubWorkflowService implements WorkflowService {

    private final Map<String, Workflow> workflows = new ConcurrentHashMap<>();

    @Override
    public String startWorkflow(StartWorkflowRequest request) {
        String workflowId = UUID.randomUUID().toString();
        Workflow workflow = StubDataGenerator.createWorkflow(
                workflowId,
                request.getName() != null ? request.getName() : "default-workflow",
                "RUNNING"
        );
        workflow.setInput(request.getInput());
        workflow.setCorrelationId(request.getCorrelationId());
        workflow.setPriority(request.getPriority() != null ? request.getPriority() : 0);
        workflows.put(workflowId, workflow);
        return workflowId;
    }

    @Override
    public Workflow getWorkflow(String workflowId, boolean includeTasks) {
        Workflow workflow = workflows.get(workflowId);
        if (workflow == null) {
            workflow = StubDataGenerator.createWorkflow(
                    workflowId, "sample-workflow", "COMPLETED");
        }
        if (!includeTasks) {
            workflow.setTasks(Collections.emptyList());
        }
        return workflow;
    }

    @Override
    public WorkflowStatus getWorkflowStatus(String workflowId) {
        Workflow workflow = getWorkflow(workflowId, false);
        WorkflowStatus status = new WorkflowStatus();
        status.setWorkflowId(workflowId);
        status.setStatus(workflow.getStatus());
        status.setCorrelationId(workflow.getCorrelationId());
        status.setOutput(workflow.getOutput());
        return status;
    }

    @Override
    public void terminateWorkflow(String workflowId, String reason) {
        Workflow workflow = workflows.get(workflowId);
        if (workflow != null) {
            workflow.setStatus("TERMINATED");
            workflow.setReasonForIncompletion(reason);
        }
    }

    @Override
    public void pauseWorkflow(String workflowId) {
        Workflow workflow = workflows.get(workflowId);
        if (workflow != null) {
            workflow.setStatus("PAUSED");
        }
    }

    @Override
    public void resumeWorkflow(String workflowId) {
        Workflow workflow = workflows.get(workflowId);
        if (workflow != null && "PAUSED".equals(workflow.getStatus())) {
            workflow.setStatus("RUNNING");
        }
    }

    @Override
    public String restartWorkflow(String workflowId, boolean useLatestDefinitions) {
        String newId = UUID.randomUUID().toString();
        Workflow oldWorkflow = workflows.get(workflowId);
        if (oldWorkflow != null) {
            Workflow newWorkflow = StubDataGenerator.createWorkflow(
                    newId, oldWorkflow.getWorkflowType(), "RUNNING"
            );
            newWorkflow.setInput(oldWorkflow.getInput());
            workflows.put(newId, newWorkflow);
        }
        return newId;
    }

    @Override
    public void retryWorkflow(String workflowId, boolean resumeSubworkflowTasks) {
        Workflow workflow = workflows.get(workflowId);
        if (workflow != null && "FAILED".equals(workflow.getStatus())) {
            workflow.setStatus("RUNNING");
        }
    }

    @Override
    public List<String> getRunningWorkflows(
            String name, Integer version, Long startTime, Long endTime) {
        return workflows.entrySet().stream()
                .filter(e -> "RUNNING".equals(e.getValue().getStatus()))
                .filter(e -> name == null || name.equals(e.getValue().getWorkflowType()))
                .map(Map.Entry::getKey)
                .toList();
    }

    @Override
    public SearchResult<WorkflowSummary> searchWorkflows(
            int start, int size, String sort, String freeText, String query) {
        List<WorkflowSummary> results = StubDataGenerator.createWorkflowSummaries(size);
        return new SearchResult<>(results.size() + 50, results);
    }

    @Override
    public SearchResult<Workflow> searchWorkflowsV2(
            int start, int size, String sort, String freeText, String query) {
        List<Workflow> results = new ArrayList<>();
        for (int i = 0; i < Math.min(size, 10); i++) {
            String status;
            if (i % 3 == 0) {
                status = "RUNNING";
            } else if (i % 3 == 1) {
                status = "COMPLETED";
            } else {
                status = "FAILED";
            }
            results.add(StubDataGenerator.createWorkflow(
                    UUID.randomUUID().toString(),
                    "sample-workflow-" + i,
                    status
            ));
        }
        return new SearchResult<>(results.size() + 50, results);
    }
}
