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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * E2E tests verifying that Temporal workflow type equals Conductor workflow name.
 *
 * This is a key verification that the DynamicWorkflow implementation correctly
 * registers workflows with Conductor workflow names (e.g., "greeting_workflow")
 * instead of the generic class name (e.g., "ConductorWorkflow").
 */
class WorkflowTypeE2ETest extends AbstractE2ETest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowTypeE2ETest.class);

    @Test
    void testWorkflowTypeEqualsWorkflowName() throws Exception {
        // Use a distinctive workflow name to verify type mapping
        String workflowName = "greeting_workflow";
        String taskName = "greet_user";

        // Register task definition
        registerTaskDefs(createTaskDefs(List.of(taskName)));

        // Register workflow definition
        WorkflowDef workflowDef = createSimpleWorkflowDef(workflowName, taskName);
        registerWorkflowDef(workflowDef);

        // Start workflow via Conductor REST API
        Map<String, Object> input = new HashMap<>();
        input.put("name", "World");
        String workflowId = startWorkflow(workflowName, 1, input);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofMinutes(1));
        assertEquals("COMPLETED", status.getStatus());

        // Get the Temporal workflow type from history
        String temporalWorkflowType = getWorkflowTypeFromHistory(workflowId);
        log.info("Temporal workflow type: {}", temporalWorkflowType);

        // Key verification: Temporal workflow type should equal Conductor workflow name
        assertEquals(workflowName, temporalWorkflowType,
                "Temporal workflow type should equal Conductor workflow name");
    }

    @Test
    void testWorkflowTypeWithVersionedWorkflow() throws Exception {
        String workflowName = "versioned_workflow";
        String taskName = "versioned_task";

        // Register task definition
        registerTaskDefs(createTaskDefs(List.of(taskName)));

        // Register workflow v1
        WorkflowDef workflowDef = createSimpleWorkflowDef(workflowName, taskName);
        workflowDef.setVersion(1);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofMinutes(1));
        assertEquals("COMPLETED", status.getStatus());

        // Verify Temporal workflow type
        String temporalWorkflowType = getWorkflowTypeFromHistory(workflowId);
        assertEquals(workflowName, temporalWorkflowType,
                "Temporal workflow type should equal Conductor workflow name regardless of version");
    }

    @Test
    void testMultipleDifferentWorkflowTypes() throws Exception {
        // Register multiple workflows with different names
        String workflow1Name = "order_workflow";
        String workflow2Name = "payment_workflow";
        String task1Name = "process_order";
        String task2Name = "process_payment";

        // Register task definitions
        registerTaskDefs(createTaskDefs(List.of(task1Name, task2Name)));

        // Register workflow definitions
        registerWorkflowDef(createSimpleWorkflowDef(workflow1Name, task1Name));
        registerWorkflowDef(createSimpleWorkflowDef(workflow2Name, task2Name));

        // Start both workflows
        String workflowId1 = startWorkflow(workflow1Name, 1, Collections.emptyMap());
        String workflowId2 = startWorkflow(workflow2Name, 1, Collections.emptyMap());

        // Wait for completion
        waitForWorkflowCompletion(workflowId1, Duration.ofMinutes(1));
        waitForWorkflowCompletion(workflowId2, Duration.ofMinutes(1));

        // Verify each workflow has its own type
        String type1 = getWorkflowTypeFromHistory(workflowId1);
        String type2 = getWorkflowTypeFromHistory(workflowId2);

        log.info("Workflow 1 type: {}", type1);
        log.info("Workflow 2 type: {}", type2);

        assertEquals(workflow1Name, type1, "First workflow type should match its name");
        assertEquals(workflow2Name, type2, "Second workflow type should match its name");
    }
}
