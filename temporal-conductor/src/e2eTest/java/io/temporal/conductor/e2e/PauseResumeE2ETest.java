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

import static org.junit.jupiter.api.Assertions.*;

/**
 * E2E tests for workflow pause and resume functionality.
 *
 * These tests verify:
 * - Pausing a running workflow sets status to PAUSED
 * - ConductorStatus search attribute is updated to PAUSED
 * - Resuming a paused workflow continues execution
 * - Workflow completes successfully after resume
 */
class PauseResumeE2ETest extends AbstractE2ETest {

    private static final Logger log = LoggerFactory.getLogger(PauseResumeE2ETest.class);

    // Slow task delay - long enough to allow pause/resume operations
    private static final long SLOW_TASK_DELAY_MS = 5000L;

    @Test
    void testPauseAndResumeWorkflow() throws Exception {
        String workflowName = "pause_resume_test_workflow";
        String taskName = "slow_task";

        // Register task definition
        registerTaskDefs(createTaskDefs(List.of(taskName)));

        // Register workflow with slow task (stays running for a while)
        WorkflowDef workflowDef = createSlowWorkflowDef(workflowName, SLOW_TASK_DELAY_MS);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());
        log.info("Started workflow with ID: {}", workflowId);

        // Wait for workflow to be RUNNING
        WorkflowStatusResponse status = waitForWorkflowStatus(workflowId, "RUNNING", Duration.ofSeconds(30));
        assertEquals("RUNNING", status.getStatus());
        log.info("Workflow is RUNNING");

        // Pause the workflow
        pauseWorkflow(workflowId);

        // Wait for status to become PAUSED
        status = waitForWorkflowStatus(workflowId, "PAUSED", Duration.ofSeconds(10));
        assertEquals("PAUSED", status.getStatus());
        log.info("Workflow is PAUSED");

        // Resume the workflow
        resumeWorkflow(workflowId);

        // Wait for status to become RUNNING again
        status = waitForWorkflowStatus(workflowId, "RUNNING", Duration.ofSeconds(10));
        assertEquals("RUNNING", status.getStatus());
        log.info("Workflow is RUNNING again after resume");

        // Wait for workflow completion (the slow task will finish)
        status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(30));
        assertEquals("COMPLETED", status.getStatus());
        log.info("Workflow COMPLETED successfully after pause/resume cycle");
    }

    @Test
    void testPausedWorkflowHasConductorStatusSearchAttribute() throws Exception {
        String workflowName = "pause_search_attr_workflow";
        String taskName = "slow_task";

        // Register task definition
        registerTaskDefs(createTaskDefs(List.of(taskName)));

        // Register workflow with slow task
        WorkflowDef workflowDef = createSlowWorkflowDef(workflowName, SLOW_TASK_DELAY_MS);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());

        // Wait for workflow to be RUNNING
        waitForWorkflowStatus(workflowId, "RUNNING", Duration.ofSeconds(30));

        // Verify initial ConductorStatus search attribute is RUNNING
        String conductorStatus = getSearchAttribute(workflowId, "ConductorStatus");
        log.info("Initial ConductorStatus search attribute: {}", conductorStatus);
        assertEquals("RUNNING", conductorStatus,
                "ConductorStatus search attribute should be RUNNING initially");

        // Pause the workflow
        pauseWorkflow(workflowId);

        // Wait for status to become PAUSED
        waitForWorkflowStatus(workflowId, "PAUSED", Duration.ofSeconds(10));

        // Verify ConductorStatus search attribute is now PAUSED
        conductorStatus = getSearchAttribute(workflowId, "ConductorStatus");
        log.info("ConductorStatus search attribute after pause: {}", conductorStatus);
        assertEquals("PAUSED", conductorStatus,
                "ConductorStatus search attribute should be PAUSED after pause");

        // Resume and wait for completion
        resumeWorkflow(workflowId);
        waitForWorkflowCompletion(workflowId, Duration.ofSeconds(30));

        // Verify final ConductorStatus is COMPLETED
        conductorStatus = getSearchAttribute(workflowId, "ConductorStatus");
        log.info("Final ConductorStatus search attribute: {}", conductorStatus);
        assertEquals("COMPLETED", conductorStatus,
                "ConductorStatus search attribute should be COMPLETED after workflow completes");
    }

    @Test
    void testMultiplePauseResumeCycles() throws Exception {
        String workflowName = "multi_pause_resume_workflow";
        String taskName = "slow_task";

        // Register task definition
        registerTaskDefs(createTaskDefs(List.of(taskName)));

        // Use a longer delay for multiple cycles
        WorkflowDef workflowDef = createSlowWorkflowDef(workflowName, 10000L);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());
        waitForWorkflowStatus(workflowId, "RUNNING", Duration.ofSeconds(30));

        // Multiple pause/resume cycles
        for (int i = 1; i <= 3; i++) {
            log.info("Pause/Resume cycle {}", i);

            pauseWorkflow(workflowId);
            WorkflowStatusResponse status = waitForWorkflowStatus(workflowId, "PAUSED", Duration.ofSeconds(10));
            assertEquals("PAUSED", status.getStatus());

            resumeWorkflow(workflowId);
            status = waitForWorkflowStatus(workflowId, "RUNNING", Duration.ofSeconds(10));
            assertEquals("RUNNING", status.getStatus());
        }

        // Wait for workflow completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(30));
        assertEquals("COMPLETED", status.getStatus());
        log.info("Workflow COMPLETED after {} pause/resume cycles", 3);
    }

    @Test
    void testPauseAlreadyCompletedWorkflow() throws Exception {
        String workflowName = "pause_completed_workflow";
        String taskName = "quick_task";

        // Register task definition
        registerTaskDefs(createTaskDefs(List.of(taskName)));

        // Register workflow with a quick task (completes immediately)
        WorkflowDef workflowDef = createSimpleWorkflowDef(workflowName, taskName);
        registerWorkflowDef(workflowDef);

        // Start workflow and wait for completion
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(30));
        assertEquals("COMPLETED", status.getStatus());
        log.info("Workflow COMPLETED");

        // Try to pause the completed workflow - should be a no-op or handled gracefully
        try {
            pauseWorkflow(workflowId);
            // If no exception, verify the workflow is still COMPLETED (pause had no effect)
            status = getWorkflowStatus(workflowId);
            assertEquals("COMPLETED", status.getStatus(),
                    "Completed workflow should remain COMPLETED after pause attempt");
            log.info("Pause on completed workflow handled gracefully - status remains COMPLETED");
        } catch (Exception e) {
            // It's acceptable to get an error when pausing a completed workflow
            log.info("Pause on completed workflow threw expected error: {}", e.getMessage());
        }
    }
}
