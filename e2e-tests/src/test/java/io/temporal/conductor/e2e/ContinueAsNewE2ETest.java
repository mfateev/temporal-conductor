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

package io.temporal.conductor.e2e;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import io.temporal.conductor.e2e.dto.WorkflowStatusResponse;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.temporal.api.enums.v1.EventType;
import io.temporal.api.history.v1.HistoryEvent;

import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * E2E tests for continue-as-new support in long-running workflows.
 *
 * <p>These tests verify that:
 * <ul>
 *   <li>Workflows with large payloads trigger continue-as-new (Temporal limits history size)</li>
 *   <li>Workflow state is preserved correctly across continue-as-new boundaries</li>
 *   <li>DO_WHILE loops complete successfully with many iterations</li>
 * </ul>
 *
 * <p>Key insight: Temporal limits workflow history SIZE, not just event count. By using
 * large payloads (~1MB per activity), we can trigger continue-as-new in under 50 iterations
 * rather than requiring 50,000+ events.
 */
@Timeout(value = 2, unit = TimeUnit.MINUTES) // Fail fast - no test should take more than 2 minutes
class ContinueAsNewE2ETest extends AbstractE2ETest {

    private static final Logger log = LoggerFactory.getLogger(ContinueAsNewE2ETest.class);

    @Test
    void testSimpleDoWhileLoop() throws Exception {
        // Test basic DO_WHILE functionality with 5 iterations
        String workflowName = "simple_do_while_workflow";
        String taskName = "simple_loop_task";
        int iterations = 5;

        // Register single task definition
        registerTaskDefs(createTaskDefs(List.of(taskName)));

        // Register workflow definition with simple DO_WHILE loop
        WorkflowDef workflowDef = createSimpleDoWhileWorkflowDef(workflowName, taskName, iterations);
        registerWorkflowDef(workflowDef);

        // Start workflow
        Map<String, Object> input = new HashMap<>();
        input.put("maxIterations", iterations);
        String workflowId = startWorkflow(workflowName, 1, input);

        log.info("Started simple DO_WHILE workflow with {} iterations, workflowId: {}", iterations, workflowId);

        // Wait for completion - fail fast
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));

        // Log detailed status for debugging
        log.info("Workflow status: {}, reasonForIncompletion: {}",
                status.getStatus(), status.getReasonForIncompletion());

        assertEquals("COMPLETED", status.getStatus(),
                "Simple DO_WHILE workflow should complete. Reason: " + status.getReasonForIncompletion());

        // Verify the loop task was executed multiple times
        List<String> activityTypes = getActivityTypesFromHistory(workflowId);
        log.info("Activity types executed: {}", activityTypes);
        assertEquals(iterations, activityTypes.size(), "Should have executed " + iterations + " iterations");

        log.info("Simple DO_WHILE workflow completed successfully");
    }

    @Test
    @Timeout(value = 4, unit = TimeUnit.MINUTES)  // Longer timeout for 1000 iterations with large payloads
    void testContinueAsNewWithLargePayloads() throws Exception {
        // Trigger continue-as-new by using large payloads
        // Temporal limits history SIZE, not just event count
        // With ~10KB payloads per activity, history grows ~30KB per iteration (input + output + metadata)
        // isContinueAsNewSuggested() triggers at ~80% of 50MB limit = ~40MB = ~1300 iterations
        String workflowName = "continue_as_new_workflow";
        String taskName = "large_payload_task";
        int iterations = 1000; // Reduced from 2000 - should still trigger continue-as-new

        // Register task definition
        registerTaskDefs(createTaskDefs(List.of(taskName)));

        // Create workflow with DO_WHILE loop that uses large payloads
        WorkflowDef workflowDef = createDoWhileWithLargePayloadsWorkflowDef(workflowName, taskName, iterations);
        registerWorkflowDef(workflowDef);

        // Generate ~10KB of data to pass to each activity iteration
        // Smaller payloads process faster in containerized environment
        // Continue-as-new should trigger at ~1300 iterations
        String largeData = generateLargeString(10 * 1024); // 10KB

        // Start workflow with large input
        Map<String, Object> input = new HashMap<>();
        input.put("largeData", largeData);
        String workflowId = startWorkflow(workflowName, 1, input);

        log.info("Started workflow {} with ~10KB payload per iteration, workflowId: {}", workflowName, workflowId);

        // Wait for completion - 3 minutes should be enough for 1000 iterations at ~10/sec
        String finalStatus = waitForWorkflowWithHistoryLogging(workflowId, Duration.ofMinutes(3));

        // Log final workflow execution info
        logWorkflowExecutionInfo(workflowId);

        // If workflow failed, log detailed error information
        if (!"COMPLETED".equals(finalStatus)) {
            log.error("Workflow ended with status: {}", finalStatus);
            logWorkflowFailureDetails(workflowId);
            // Log conductor server logs for debugging
            logConductorServerLogs();
        }

        assertEquals("COMPLETED", finalStatus,
                "Workflow with large payloads should complete");

        // Check that continue-as-new occurred
        boolean continueAsNewOccurred = checkContinueAsNewOccurred(workflowId);
        log.info("Continue-as-new occurred: {}", continueAsNewOccurred);

        assertTrue(continueAsNewOccurred,
                "Continue-as-new should occur with large payloads");

        log.info("Workflow with large payloads completed successfully with continue-as-new");
    }

    @Test
    void testDoWhileLoopWithManyIterations() throws Exception {
        // Test DO_WHILE loop with many iterations (without triggering continue-as-new)
        // This validates the loop implementation without large payloads
        String workflowName = "do_while_loop_workflow";
        String taskName = "loop_task";
        int iterations = 200;  // Increased to test beyond 85

        // Register single task definition
        registerTaskDefs(createTaskDefs(List.of(taskName)));

        // Register workflow definition with DO_WHILE loop
        WorkflowDef workflowDef = createSimpleDoWhileWorkflowDef(workflowName, taskName, iterations);
        registerWorkflowDef(workflowDef);

        // Start workflow
        Map<String, Object> input = new HashMap<>();
        String workflowId = startWorkflow(workflowName, 1, input);

        log.info("Started workflow {} with {} iterations, workflowId: {}", workflowName, iterations, workflowId);

        // Wait for completion - fail fast
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(90));

        // Only log on failure to avoid timeout overhead
        if (!"COMPLETED".equals(status.getStatus())) {
            log.info("=== Conductor Server Logs ===");
            logConductorServerLogs();
            log.info("=== End Conductor Server Logs ===");
        }

        assertEquals("COMPLETED", status.getStatus(), "Workflow with " + iterations + " iterations should complete");

        // Verify all iterations completed by checking activity count
        List<String> activityTypes = getActivityTypesFromHistory(workflowId);

        // Log DO_WHILE details if count doesn't match
        if (activityTypes.size() != iterations) {
            log.error("Expected {} activities but got {}. Logging workflow details...", iterations, activityTypes.size());
            logWorkflowExecutionInfo(workflowId);
        }

        assertEquals(iterations, activityTypes.size(), "Should have executed " + iterations + " activities");

        log.info("Workflow with {} DO_WHILE iterations completed successfully", iterations);
    }

    @Test
    void testWorkflowWithManySequentialTasks() throws Exception {
        // Create a workflow with many sequential tasks to exercise state management
        String workflowName = "many_tasks_workflow";
        int numTasks = 10;

        List<String> taskNames = new ArrayList<>();
        for (int i = 1; i <= numTasks; i++) {
            taskNames.add("task_" + i);
        }

        // Register task definitions
        registerTaskDefs(createTaskDefs(taskNames));

        // Register workflow definition
        WorkflowDef workflowDef = createSequentialWorkflowDef(workflowName, taskNames);
        registerWorkflowDef(workflowDef);

        // Start workflow
        Map<String, Object> input = new HashMap<>();
        input.put("startValue", 100);
        String workflowId = startWorkflow(workflowName, 1, input);

        // Wait for completion - fail fast
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus(), "Workflow with many tasks should complete");

        // Verify all tasks completed
        List<String> activityTypes = getActivityTypesFromHistory(workflowId);
        assertEquals(numTasks, activityTypes.size(), "All tasks should have been executed");

        // Verify task types match task names
        for (int i = 0; i < numTasks; i++) {
            assertEquals("task_" + (i + 1), activityTypes.get(i),
                    "Activity type should match Conductor task name");
        }

        log.info("Workflow with {} tasks completed successfully", numTasks);
    }

    @Test
    void testWorkflowWithSetVariable() throws Exception {
        // Test SET_VARIABLE task type which is used for state preservation
        String workflowName = "set_variable_workflow";

        // Create workflow with SET_VARIABLE and a subsequent task that uses the variable
        WorkflowDef workflowDef = createWorkflowWithSetVariable(workflowName);

        // Register task definitions for the SIMPLE tasks
        List<TaskDef> taskDefs = new ArrayList<>();
        TaskDef taskDef = new TaskDef();
        taskDef.setName("use_variable_task");
        taskDefs.add(taskDef);
        registerTaskDefs(taskDefs);

        // Register workflow definition
        registerWorkflowDef(workflowDef);

        // Start workflow
        Map<String, Object> input = new HashMap<>();
        input.put("initialValue", 42);
        String workflowId = startWorkflow(workflowName, 1, input);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(30));
        assertEquals("COMPLETED", status.getStatus(), "Workflow with SET_VARIABLE should complete");

        log.info("Workflow with SET_VARIABLE completed successfully");
    }

    @Test
    void testLongRunningWorkflowWithVariableUpdates() throws Exception {
        // Test a workflow that updates variables multiple times
        // This exercises the variable preservation code used in continue-as-new
        String workflowName = "variable_updates_workflow";

        WorkflowDef workflowDef = createWorkflowWithMultipleVariableUpdates(workflowName);

        // Register task definitions
        List<TaskDef> taskDefs = new ArrayList<>();
        TaskDef taskDef = new TaskDef();
        taskDef.setName("compute_task");
        taskDefs.add(taskDef);
        registerTaskDefs(taskDefs);

        // Register workflow definition
        registerWorkflowDef(workflowDef);

        // Start workflow
        Map<String, Object> input = new HashMap<>();
        input.put("input_value", 10);
        String workflowId = startWorkflow(workflowName, 1, input);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(30));
        assertEquals("COMPLETED", status.getStatus(), "Workflow with variable updates should complete");

        // Get workflow output (variables are often reflected in output)
        assertNotNull(status.getOutput(), "Workflow should have output");

        log.info("Workflow with multiple variable updates completed successfully");
    }

    @Test
    void testWorkflowStatePreservationAcrossTasks() throws Exception {
        // Test that task outputs are correctly tracked across multiple tasks
        // This validates the taskOutputs preservation used in continue-as-new
        String workflowName = "state_preservation_workflow";

        List<String> taskNames = List.of("first_task", "second_task", "third_task");

        // Register task definitions
        registerTaskDefs(createTaskDefs(taskNames));

        // Create workflow where each task references previous task's output
        WorkflowDef workflowDef = createWorkflowWithOutputReferences(workflowName, taskNames);
        registerWorkflowDef(workflowDef);

        // Start workflow
        Map<String, Object> input = new HashMap<>();
        input.put("initial", "start");
        String workflowId = startWorkflow(workflowName, 1, input);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(30));
        assertEquals("COMPLETED", status.getStatus(), "Workflow with output references should complete");

        // Verify all tasks executed
        List<String> activityTypes = getActivityTypesFromHistory(workflowId);
        assertEquals(3, activityTypes.size(), "All three tasks should have executed");

        log.info("Workflow with state preservation completed successfully");
    }

    // ==================== Helper Methods ====================

    /**
     * Generate a large string of the specified size for testing large payloads.
     */
    private String generateLargeString(int sizeInBytes) {
        StringBuilder sb = new StringBuilder(sizeInBytes);
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        for (int i = 0; i < sizeInBytes; i++) {
            sb.append(chars.charAt(i % chars.length()));
        }
        return sb.toString();
    }

    /**
     * Create a workflow definition with DO_WHILE loop that uses large payloads.
     * The large data is passed to each activity, causing history size to grow rapidly.
     */
    private WorkflowDef createDoWhileWithLargePayloadsWorkflowDef(String workflowName, String taskName, int iterations) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        List<WorkflowTask> tasks = new ArrayList<>();

        // DO_WHILE loop with large payload per iteration
        WorkflowTask doWhile = new WorkflowTask();
        doWhile.setName("loop");
        doWhile.setTaskReferenceName("loop_ref");
        doWhile.setType(TaskType.DO_WHILE.name());
        doWhile.setLoopCondition("if ($.loop_ref['iteration'] < " + iterations + ") { true; } else { false; }");

        // Task inside the loop - passes large data to each activity
        WorkflowTask loopTask = new WorkflowTask();
        loopTask.setName(taskName);
        loopTask.setTaskReferenceName(taskName + "_ref");
        loopTask.setType(TaskType.SIMPLE.name());
        Map<String, Object> loopTaskInput = new HashMap<>();
        loopTaskInput.put("iteration", "${loop_ref.iteration}");
        // Pass the large data to each activity - this grows the history
        loopTaskInput.put("largeData", "${workflow.input.largeData}");
        loopTask.setInputParameters(loopTaskInput);

        doWhile.setLoopOver(Collections.singletonList(loopTask));
        tasks.add(doWhile);

        workflowDef.setTasks(tasks);
        return workflowDef;
    }

    /**
     * Create a workflow definition with DO_WHILE loop.
     */
    private WorkflowDef createDoWhileWorkflowDef(String workflowName, String taskName, int iterations) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        List<WorkflowTask> tasks = new ArrayList<>();

        // SET_VARIABLE to initialize counter
        WorkflowTask initCounter = new WorkflowTask();
        initCounter.setName("init_counter");
        initCounter.setTaskReferenceName("init_counter_ref");
        initCounter.setType(TaskType.SET_VARIABLE.name());
        Map<String, Object> initInput = new HashMap<>();
        initInput.put("counter", 0);
        initCounter.setInputParameters(initInput);
        tasks.add(initCounter);

        // DO_WHILE loop
        WorkflowTask doWhile = new WorkflowTask();
        doWhile.setName("loop");
        doWhile.setTaskReferenceName("loop_ref");
        doWhile.setType(TaskType.DO_WHILE.name());
        // Loop condition: counter < maxIterations
        doWhile.setLoopCondition("if ($.loop_ref['iteration'] < $.workflow.input.maxIterations) { true; } else { false; }");

        // Task inside the loop
        WorkflowTask loopTask = new WorkflowTask();
        loopTask.setName(taskName);
        loopTask.setTaskReferenceName(taskName + "_ref");
        loopTask.setType(TaskType.SIMPLE.name());
        Map<String, Object> loopTaskInput = new HashMap<>();
        loopTaskInput.put("iteration", "${loop_ref.iteration}");
        loopTask.setInputParameters(loopTaskInput);

        doWhile.setLoopOver(Collections.singletonList(loopTask));
        tasks.add(doWhile);

        workflowDef.setTasks(tasks);
        return workflowDef;
    }

    /**
     * Create a simple workflow definition with DO_WHILE loop (no SET_VARIABLE, simpler condition).
     */
    private WorkflowDef createSimpleDoWhileWorkflowDef(String workflowName, String taskName, int iterations) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        List<WorkflowTask> tasks = new ArrayList<>();

        // DO_WHILE loop with simple condition
        WorkflowTask doWhile = new WorkflowTask();
        doWhile.setName("loop");
        doWhile.setTaskReferenceName("loop_ref");
        doWhile.setType(TaskType.DO_WHILE.name());
        // Simple loop condition using iteration directly
        doWhile.setLoopCondition("if ($.loop_ref['iteration'] < " + iterations + ") { true; } else { false; }");

        // Task inside the loop
        WorkflowTask loopTask = new WorkflowTask();
        loopTask.setName(taskName);
        loopTask.setTaskReferenceName(taskName + "_ref");
        loopTask.setType(TaskType.SIMPLE.name());
        Map<String, Object> loopTaskInput = new HashMap<>();
        loopTaskInput.put("iteration", "${loop_ref.iteration}");
        loopTask.setInputParameters(loopTaskInput);

        doWhile.setLoopOver(Collections.singletonList(loopTask));
        tasks.add(doWhile);

        workflowDef.setTasks(tasks);
        return workflowDef;
    }

    /**
     * Log workflow execution info from Temporal (history size, event count, etc.)
     */
    private void logWorkflowExecutionInfo(String workflowId) {
        try {
            var response = workflowServiceStubs.blockingStub()
                    .describeWorkflowExecution(
                            io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest.newBuilder()
                                    .setNamespace(NAMESPACE)
                                    .setExecution(io.temporal.api.common.v1.WorkflowExecution.newBuilder()
                                            .setWorkflowId(workflowId)
                                            .build())
                                    .build());

            var info = response.getWorkflowExecutionInfo();
            log.info("Workflow execution info for {}:", workflowId);
            log.info("  Status: {}", info.getStatus());
            log.info("  History length: {} events", info.getHistoryLength());
            log.info("  History size: {} bytes ({} KB)", info.getHistorySizeBytes(), info.getHistorySizeBytes() / 1024);
            log.info("  Execution time: {}", info.getExecutionTime());
        } catch (Exception e) {
            log.warn("Could not get workflow execution info: {}", e.getMessage());
        }
    }

    /**
     * Log detailed workflow failure information from Temporal history.
     */
    private void logWorkflowFailureDetails(String workflowId) {
        try {
            List<HistoryEvent> events = getWorkflowHistory(workflowId);
            for (HistoryEvent event : events) {
                EventType type = event.getEventType();
                if (type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED) {
                    var attrs = event.getWorkflowExecutionFailedEventAttributes();
                    log.error("Workflow failed: {}", attrs.getFailure().getMessage());
                    if (attrs.getFailure().hasCause()) {
                        log.error("  Cause: {}", attrs.getFailure().getCause().getMessage());
                    }
                } else if (type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED) {
                    var attrs = event.getWorkflowExecutionTerminatedEventAttributes();
                    log.error("Workflow terminated: {}", attrs.getReason());
                } else if (type == EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED) {
                    var attrs = event.getActivityTaskFailedEventAttributes();
                    log.error("Activity failed: {}", attrs.getFailure().getMessage());
                } else if (type == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED) {
                    var attrs = event.getWorkflowTaskFailedEventAttributes();
                    log.error("Workflow task failed: {}", attrs.getFailure().getMessage());
                }
            }
        } catch (Exception e) {
            log.warn("Could not get workflow failure details: {}", e.getMessage());
        }
    }

    /**
     * Check if continue-as-new occurred by looking at workflow history across all runs.
     * The WORKFLOW_EXECUTION_CONTINUED_AS_NEW event is at the END of a previous run's history,
     * so we need to check all runs, not just the current one.
     */
    private boolean checkContinueAsNewOccurred(String workflowId) {
        try {
            // Use getWorkflowHistoryAllRuns to get history from all continue-as-new runs
            List<HistoryEvent> events = getWorkflowHistoryAllRuns(workflowId);
            int continueAsNewCount = 0;
            for (HistoryEvent event : events) {
                if (event.getEventType() == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW) {
                    continueAsNewCount++;
                    log.info("Found WORKFLOW_EXECUTION_CONTINUED_AS_NEW event at eventId: {}", event.getEventId());
                }
            }
            if (continueAsNewCount > 0) {
                log.info("Continue-as-new occurred {} times", continueAsNewCount);
                return true;
            }
            return false;
        } catch (Exception e) {
            log.warn("Could not check for continue-as-new: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Wait for workflow completion while periodically logging history size.
     * This provides visibility into why workflows may fail to trigger continue-as-new.
     *
     * @param workflowId the workflow ID to poll
     * @param timeout maximum time to wait
     * @return the final workflow status
     */
    private String waitForWorkflowWithHistoryLogging(String workflowId, Duration timeout) {
        long startTime = System.currentTimeMillis();
        long timeoutMillis = timeout.toMillis();
        long lastLogTime = 0;
        long logIntervalMillis = 5000; // Log every 5 seconds
        int pollCount = 0;
        String lastStatus = "UNKNOWN";

        while ((System.currentTimeMillis() - startTime) < timeoutMillis) {
            pollCount++;

            // Get workflow status from Conductor
            try {
                var status = getWorkflowStatus(workflowId);
                lastStatus = status.getStatus();

                // Check for terminal status
                if ("COMPLETED".equals(lastStatus) || "FAILED".equals(lastStatus)
                        || "TERMINATED".equals(lastStatus) || "TIMED_OUT".equals(lastStatus)) {
                    log.info("Workflow {} reached terminal status: {} after {} polls", workflowId, lastStatus, pollCount);
                    logWorkflowExecutionInfo(workflowId);
                    return lastStatus;
                }
            } catch (Exception e) {
                log.warn("Error getting workflow status: {}", e.getMessage());
            }

            // Log history size periodically
            long now = System.currentTimeMillis();
            if ((now - lastLogTime) >= logIntervalMillis) {
                lastLogTime = now;
                logHistoryProgress(workflowId, pollCount, startTime);
            }

            // Short poll interval
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for workflow", e);
            }
        }

        // Timeout reached
        long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
        log.error("Timeout waiting for workflow {} after {} seconds. Last status: {}", workflowId, elapsedSeconds, lastStatus);
        logWorkflowExecutionInfo(workflowId);
        logWorkflowFailureDetails(workflowId);

        return lastStatus;
    }

    /**
     * Log conductor server logs for debugging continue-as-new issues.
     */
    private void logConductorServerLogs() {
        try {
            String logs = composeContainer.getContainerByServiceName("conductor-server")
                    .map(container -> container.getLogs())
                    .orElse("No conductor-server container found");

            // Filter for workflow-related logs
            String[] lines = logs.split("\n");
            log.info("=== Conductor Server Logs (last 100 workflow lines) ===");
            int count = 0;
            for (int i = lines.length - 1; i >= 0 && count < 100; i--) {
                String line = lines[i];
                if (line.contains("ConductorWorkflow") || line.contains("historySize") ||
                    line.contains("Continue-as-new") || line.contains("Scheduling loop") ||
                    line.contains("iteration")) {
                    log.info("  {}", line);
                    count++;
                }
            }
            log.info("=== End Conductor Server Logs ===");
        } catch (Exception e) {
            log.warn("Could not get conductor server logs: {}", e.getMessage());
        }
    }

    /**
     * Log current history progress including size, event count, and activities completed.
     */
    private void logHistoryProgress(String workflowId, int pollCount, long startTime) {
        try {
            var response = workflowServiceStubs.blockingStub()
                    .describeWorkflowExecution(
                            io.temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest.newBuilder()
                                    .setNamespace(NAMESPACE)
                                    .setExecution(io.temporal.api.common.v1.WorkflowExecution.newBuilder()
                                            .setWorkflowId(workflowId)
                                            .build())
                                    .build());

            var info = response.getWorkflowExecutionInfo();
            long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
            long historySizeKB = info.getHistorySizeBytes() / 1024;
            long historySizeMB = historySizeKB / 1024;

            // Count activities from history
            int activityCount = 0;
            try {
                activityCount = getActivityTypesFromHistory(workflowId).size();
            } catch (Exception e) {
                // Ignore errors counting activities
            }

            log.info("[{}s] Workflow {} - Status: {}, History: {} events / {} KB ({} MB), Activities: {}",
                    elapsedSeconds, workflowId, info.getStatus(),
                    info.getHistoryLength(), historySizeKB, historySizeMB, activityCount);

            // Warn if approaching history limit (default is around 50MB)
            if (historySizeMB > 40) {
                log.warn("History size approaching limit! {} MB - continue-as-new should trigger soon", historySizeMB);
            }
        } catch (Exception e) {
            log.debug("Could not get history progress: {}", e.getMessage());
        }
    }

    /**
     * Create a workflow definition with SET_VARIABLE task.
     */
    private WorkflowDef createWorkflowWithSetVariable(String workflowName) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        List<WorkflowTask> tasks = new ArrayList<>();

        // SET_VARIABLE task
        WorkflowTask setVarTask = new WorkflowTask();
        setVarTask.setName("set_computed_value");
        setVarTask.setTaskReferenceName("set_computed_value_ref");
        setVarTask.setType(TaskType.SET_VARIABLE.name());
        Map<String, Object> setVarInput = new HashMap<>();
        setVarInput.put("computedValue", "${workflow.input.initialValue}");
        setVarInput.put("timestamp", "${workflow.createTime}");
        setVarTask.setInputParameters(setVarInput);
        tasks.add(setVarTask);

        // Task that uses the variable
        WorkflowTask useVarTask = new WorkflowTask();
        useVarTask.setName("use_variable_task");
        useVarTask.setTaskReferenceName("use_variable_task_ref");
        useVarTask.setType(TaskType.SIMPLE.name());
        Map<String, Object> useVarInput = new HashMap<>();
        useVarInput.put("value", "${workflow.variables.computedValue}");
        useVarTask.setInputParameters(useVarInput);
        tasks.add(useVarTask);

        workflowDef.setTasks(tasks);
        return workflowDef;
    }

    /**
     * Create a workflow with multiple SET_VARIABLE tasks.
     */
    private WorkflowDef createWorkflowWithMultipleVariableUpdates(String workflowName) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        List<WorkflowTask> tasks = new ArrayList<>();

        // First SET_VARIABLE
        WorkflowTask setVar1 = new WorkflowTask();
        setVar1.setName("set_initial");
        setVar1.setTaskReferenceName("set_initial_ref");
        setVar1.setType(TaskType.SET_VARIABLE.name());
        Map<String, Object> setVar1Input = new HashMap<>();
        setVar1Input.put("counter", "${workflow.input.input_value}");
        setVar1.setInputParameters(setVar1Input);
        tasks.add(setVar1);

        // Compute task
        WorkflowTask computeTask = new WorkflowTask();
        computeTask.setName("compute_task");
        computeTask.setTaskReferenceName("compute_task_ref");
        computeTask.setType(TaskType.SIMPLE.name());
        Map<String, Object> computeInput = new HashMap<>();
        computeInput.put("currentValue", "${workflow.variables.counter}");
        computeTask.setInputParameters(computeInput);
        tasks.add(computeTask);

        // Second SET_VARIABLE (update based on compute result)
        WorkflowTask setVar2 = new WorkflowTask();
        setVar2.setName("update_counter");
        setVar2.setTaskReferenceName("update_counter_ref");
        setVar2.setType(TaskType.SET_VARIABLE.name());
        Map<String, Object> setVar2Input = new HashMap<>();
        setVar2Input.put("counter", "${compute_task_ref.output.result}");
        setVar2Input.put("completed", true);
        setVar2.setInputParameters(setVar2Input);
        tasks.add(setVar2);

        workflowDef.setTasks(tasks);
        return workflowDef;
    }

    /**
     * Create a workflow where tasks reference previous task outputs.
     */
    private WorkflowDef createWorkflowWithOutputReferences(String workflowName, List<String> taskNames) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(workflowName);
        workflowDef.setVersion(1);

        List<WorkflowTask> tasks = new ArrayList<>();
        String previousTaskRef = null;

        for (String taskName : taskNames) {
            WorkflowTask task = new WorkflowTask();
            task.setName(taskName);
            task.setTaskReferenceName(taskName + "_ref");
            task.setType(TaskType.SIMPLE.name());

            Map<String, Object> inputParams = new HashMap<>();
            if (previousTaskRef == null) {
                // First task uses workflow input
                inputParams.put("input", "${workflow.input.initial}");
            } else {
                // Subsequent tasks reference previous task output
                inputParams.put("input", "${" + previousTaskRef + ".output.result}");
            }
            task.setInputParameters(inputParams);
            tasks.add(task);

            previousTaskRef = taskName + "_ref";
        }

        workflowDef.setTasks(tasks);
        return workflowDef;
    }
}
