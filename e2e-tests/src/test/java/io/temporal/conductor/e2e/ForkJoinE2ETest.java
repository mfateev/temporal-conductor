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

import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import io.temporal.conductor.e2e.dto.WorkflowStatusResponse;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * E2E tests for FORK_JOIN parallel execution against real Temporal server.
 */
class ForkJoinE2ETest extends AbstractE2ETest {

    private static final Logger log = LoggerFactory.getLogger(ForkJoinE2ETest.class);

    @Test
    void testForkJoinExecution() throws Exception {
        String workflowName = "fork_join_e2e_workflow";
        List<String> branchTasks = List.of("branch_a", "branch_b", "branch_c");
        String finalTask = "final_task";

        // Register all tasks
        List<String> allTasks = new java.util.ArrayList<>(branchTasks);
        allTasks.add(finalTask);
        registerTaskDefs(createTaskDefs(allTasks));

        // Register workflow with FORK_JOIN
        WorkflowDef workflowDef = createForkJoinWorkflowDef(workflowName, branchTasks, finalTask);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(30));
        assertEquals("COMPLETED", status.getStatus());

        // Verify all tasks completed
        assertNotNull(status.getTasks());

        Set<String> completedTaskRefs = status.getTasks().stream()
                .filter(t -> "COMPLETED".equals(t.getStatus()))
                .map(WorkflowStatusResponse.TaskInfo::getReferenceTaskName)
                .collect(Collectors.toSet());

        log.info("Completed task references: {}", completedTaskRefs);

        // All branch tasks should be completed
        for (String branchTask : branchTasks) {
            assertTrue(completedTaskRefs.contains(branchTask + "_ref"),
                    "Branch task should be completed: " + branchTask);
        }

        // Final task after JOIN should be completed
        assertTrue(completedTaskRefs.contains(finalTask + "_ref"),
                "Final task should be completed: " + finalTask);
    }

    @Test
    void testForkJoinParallelScheduling() throws Exception {
        String workflowName = "fork_parallel_e2e_workflow";
        List<String> branchTasks = List.of("parallel_a", "parallel_b", "parallel_c");
        String finalTask = "after_join_task";

        // Register all tasks
        List<String> allTasks = new java.util.ArrayList<>(branchTasks);
        allTasks.add(finalTask);
        registerTaskDefs(createTaskDefs(allTasks));

        // Register workflow with FORK_JOIN
        WorkflowDef workflowDef = createForkJoinWorkflowDef(workflowName, branchTasks, finalTask);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(30));
        assertEquals("COMPLETED", status.getStatus());

        // Verify parallel scheduling by checking consecutive ActivityTaskScheduled events
        int maxConsecutive = getMaxConsecutiveActivityScheduledEvents(workflowId);
        log.info("Max consecutive ActivityTaskScheduled events: {}", maxConsecutive);

        // With 3 parallel branches, we should see at least 3 consecutive ActivityTaskScheduled events
        assertTrue(maxConsecutive >= 3,
                "Expected at least 3 consecutive ActivityTaskScheduled events for parallel FORK " +
                        "branch scheduling, but found " + maxConsecutive + ". " +
                        "This indicates FORK branches may be scheduled sequentially instead of in parallel.");
    }

    @Test
    void testForkJoinActivityTypesAreTaskNames() throws Exception {
        String workflowName = "fork_activity_type_workflow";
        List<String> branchTasks = List.of("compute_a", "compute_b");
        String finalTask = "aggregate";

        // Register all tasks
        List<String> allTasks = new java.util.ArrayList<>(branchTasks);
        allTasks.add(finalTask);
        registerTaskDefs(createTaskDefs(allTasks));

        // Register workflow with FORK_JOIN
        WorkflowDef workflowDef = createForkJoinWorkflowDef(workflowName, branchTasks, finalTask);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());

        // Wait for completion
        waitForWorkflowCompletion(workflowId, Duration.ofSeconds(30));

        // Get activity types from Temporal history
        List<String> activityTypes = getActivityTypesFromHistory(workflowId);
        log.info("Activity types from FORK_JOIN workflow: {}", activityTypes);

        // Verify all task names appear as activity types
        for (String taskName : allTasks) {
            assertTrue(activityTypes.contains(taskName),
                    "Activity type should include task: " + taskName);
        }

        // Verify no generic activity types
        for (String activityType : activityTypes) {
            assertNotEquals("executeTask", activityType,
                    "Activity type should not be generic 'executeTask'");
        }
    }

    @Test
    void testForkJoinWithManyBranches() throws Exception {
        String workflowName = "fork_many_branches_workflow";
        List<String> branchTasks = List.of("worker_1", "worker_2", "worker_3", "worker_4", "worker_5");
        String finalTask = "collector";

        // Register all tasks
        List<String> allTasks = new java.util.ArrayList<>(branchTasks);
        allTasks.add(finalTask);
        registerTaskDefs(createTaskDefs(allTasks));

        // Register workflow with FORK_JOIN
        WorkflowDef workflowDef = createForkJoinWorkflowDef(workflowName, branchTasks, finalTask);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus());

        // Verify parallel scheduling
        int maxConsecutive = getMaxConsecutiveActivityScheduledEvents(workflowId);
        log.info("Max consecutive ActivityTaskScheduled events for 5 branches: {}", maxConsecutive);

        // With 5 parallel branches, we should see at least 5 consecutive ActivityTaskScheduled events
        assertTrue(maxConsecutive >= 5,
                "Expected at least 5 consecutive ActivityTaskScheduled events for 5-branch FORK, " +
                        "but found " + maxConsecutive);

        // Verify all activities scheduled
        List<String> activityTypes = getActivityTypesFromHistory(workflowId);
        assertEquals(allTasks.size(), activityTypes.size(),
                "Should have activity for each task including branches and final task");
    }
}
