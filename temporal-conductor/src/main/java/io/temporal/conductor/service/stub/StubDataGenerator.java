package io.temporal.conductor.service.stub;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import io.temporal.conductor.dto.TaskSummary;
import io.temporal.conductor.dto.Workflow;
import io.temporal.conductor.dto.WorkflowSummary;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Generates realistic mock data for stub services.
 */
public final class StubDataGenerator {

    private StubDataGenerator() {
    }

    /**
     * Create a workflow with sample data.
     */
    public static Workflow createWorkflow(String workflowId, String workflowType, String status) {
        Workflow workflow = new Workflow();
        workflow.setWorkflowId(workflowId);
        workflow.setWorkflowType(workflowType);
        workflow.setVersion(1);
        workflow.setStatus(status);
        workflow.setCreateTime(Instant.now().minusSeconds(3600).toEpochMilli());
        workflow.setUpdateTime(Instant.now().toEpochMilli());
        workflow.setInput(Map.of("key1", "value1", "key2", 123));
        workflow.setOutput(Map.of("result", "success"));
        workflow.setCorrelationId("correlation-" + workflowId.substring(0, 8));
        workflow.setPriority(5);
        workflow.setOwnerApp("test-app");
        workflow.setCreatedBy("test-user");

        // Add sample tasks
        workflow.setTasks(createTasks(workflowId, status));

        // Add workflow definition
        workflow.setWorkflowDefinition(createWorkflowDef(workflowType));

        return workflow;
    }

    /**
     * Create sample tasks for a workflow.
     */
    public static List<Task> createTasks(String workflowId, String workflowStatus) {
        List<Task> tasks = new ArrayList<>();

        Task task1 = new Task();
        task1.setTaskId(workflowId + "-task-1");
        task1.setTaskType("SIMPLE");
        task1.setTaskDefName("simple_task_1");
        task1.setReferenceTaskName("task1_ref");
        task1.setStatus("RUNNING".equals(workflowStatus)
                ? Task.Status.IN_PROGRESS : Task.Status.COMPLETED);
        task1.setScheduledTime(System.currentTimeMillis() - 60000);
        task1.setStartTime(System.currentTimeMillis() - 55000);
        task1.setEndTime("RUNNING".equals(workflowStatus)
                ? 0 : System.currentTimeMillis() - 50000);
        task1.setInputData(Map.of("input1", "value1"));
        task1.setOutputData(Map.of("output1", "result1"));
        task1.setWorkflowInstanceId(workflowId);
        tasks.add(task1);

        Task task2 = new Task();
        task2.setTaskId(workflowId + "-task-2");
        task2.setTaskType("SIMPLE");
        task2.setTaskDefName("simple_task_2");
        task2.setReferenceTaskName("task2_ref");
        task2.setStatus("RUNNING".equals(workflowStatus)
                ? Task.Status.SCHEDULED : Task.Status.COMPLETED);
        task2.setScheduledTime(System.currentTimeMillis() - 30000);
        task2.setWorkflowInstanceId(workflowId);
        tasks.add(task2);

        return tasks;
    }

    /**
     * Create a sample workflow definition.
     */
    public static WorkflowDef createWorkflowDef(String name) {
        WorkflowDef def = new WorkflowDef();
        def.setName(name);
        def.setVersion(1);
        def.setDescription("Sample workflow definition for " + name);
        def.setOwnerEmail("owner@example.com");
        def.setTimeoutSeconds(3600);

        List<WorkflowTask> tasks = new ArrayList<>();

        WorkflowTask task1 = new WorkflowTask();
        task1.setName("simple_task_1");
        task1.setTaskReferenceName("task1_ref");
        task1.setType("SIMPLE");
        tasks.add(task1);

        WorkflowTask task2 = new WorkflowTask();
        task2.setName("simple_task_2");
        task2.setTaskReferenceName("task2_ref");
        task2.setType("SIMPLE");
        tasks.add(task2);

        def.setTasks(tasks);
        return def;
    }

    /**
     * Create a list of workflow summaries.
     */
    public static List<WorkflowSummary> createWorkflowSummaries(int count) {
        List<WorkflowSummary> summaries = new ArrayList<>();
        String[] statuses = {"RUNNING", "COMPLETED", "FAILED", "PAUSED"};

        for (int i = 0; i < count; i++) {
            WorkflowSummary summary = new WorkflowSummary();
            summary.setWorkflowId(UUID.randomUUID().toString());
            summary.setWorkflowType("sample-workflow-" + (i % 5));
            summary.setVersion(1);
            summary.setStatus(statuses[i % statuses.length]);
            summary.setStartTime(Instant.now().minusSeconds(3600 - i * 60L).toEpochMilli());
            summary.setUpdateTime(Instant.now().minusSeconds(i * 30L).toEpochMilli());
            summary.setCorrelationId("correlation-" + i);
            summary.setPriority(i % 10);
            summaries.add(summary);
        }

        return summaries;
    }

    /**
     * Create a list of sample workflow definitions.
     */
    public static List<WorkflowDef> createWorkflowDefs() {
        List<WorkflowDef> defs = new ArrayList<>();
        defs.add(createWorkflowDef("order-processing"));
        defs.add(createWorkflowDef("payment-workflow"));
        defs.add(createWorkflowDef("notification-workflow"));
        defs.add(createWorkflowDef("data-pipeline"));
        return defs;
    }

    /**
     * Create a list of sample task definitions.
     */
    public static List<TaskDef> createTaskDefs() {
        List<TaskDef> defs = new ArrayList<>();

        TaskDef task1 = new TaskDef();
        task1.setName("simple_task_1");
        task1.setDescription("Simple task 1");
        task1.setRetryCount(3);
        task1.setTimeoutSeconds(60);
        defs.add(task1);

        TaskDef task2 = new TaskDef();
        task2.setName("simple_task_2");
        task2.setDescription("Simple task 2");
        task2.setRetryCount(3);
        task2.setTimeoutSeconds(120);
        defs.add(task2);

        TaskDef httpTask = new TaskDef();
        httpTask.setName("http_task");
        httpTask.setDescription("HTTP task");
        httpTask.setRetryCount(2);
        httpTask.setTimeoutSeconds(30);
        defs.add(httpTask);

        return defs;
    }

    /**
     * Create a list of task summaries.
     */
    public static List<TaskSummary> createTaskSummaries(int count) {
        List<TaskSummary> summaries = new ArrayList<>();
        String[] statuses = {"IN_PROGRESS", "COMPLETED", "FAILED", "SCHEDULED"};
        String[] taskTypes = {"SIMPLE", "HTTP", "FORK", "JOIN"};

        for (int i = 0; i < count; i++) {
            TaskSummary summary = new TaskSummary();
            summary.setTaskId(UUID.randomUUID().toString());
            summary.setWorkflowId(UUID.randomUUID().toString());
            summary.setWorkflowType("sample-workflow-" + (i % 3));
            summary.setTaskDefName("task_" + (i % 5));
            summary.setTaskType(taskTypes[i % taskTypes.length]);
            summary.setStatus(statuses[i % statuses.length]);
            summary.setScheduledTime(Instant.now().minusSeconds(3600 - i * 60L).toString());
            summary.setStartTime(Instant.now().minusSeconds(3500 - i * 60L).toString());
            summary.setUpdateTime(Instant.now().minusSeconds(i * 30L).toString());
            summary.setWorkflowPriority(i % 10);
            summaries.add(summary);
        }

        return summaries;
    }
}
