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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * E2E tests verifying that Temporal activity type equals Conductor task name.
 *
 * This verifies that activities are registered with Conductor task names
 * (e.g., "process_order") instead of generic names (e.g., "executeTask").
 */
class ActivityTypeE2ETest extends AbstractE2ETest {

    private static final Logger log = LoggerFactory.getLogger(ActivityTypeE2ETest.class);

    @Test
    void testActivityTypeEqualsTaskName() throws Exception {
        String workflowName = "activity_type_test_workflow";
        String taskName = "process_order";

        // Register task definition
        registerTaskDefs(createTaskDefs(List.of(taskName)));

        // Register workflow definition
        WorkflowDef workflowDef = createSimpleWorkflowDef(workflowName, taskName);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofMinutes(1));
        assertEquals("COMPLETED", status.getStatus());

        // Get activity types from Temporal history
        List<String> activityTypes = getActivityTypesFromHistory(workflowId);
        log.info("Activity types from history: {}", activityTypes);

        // Verify activity type equals task name
        assertEquals(1, activityTypes.size(), "Should have exactly one activity");
        assertEquals(taskName, activityTypes.get(0),
                "Temporal activity type should equal Conductor task name");
    }

    @Test
    void testMultipleActivityTypes() throws Exception {
        String workflowName = "multi_activity_workflow";
        List<String> taskNames = List.of("validate_input", "process_data", "send_notification");

        // Register task definitions
        registerTaskDefs(createTaskDefs(taskNames));

        // Register workflow with sequential tasks
        WorkflowDef workflowDef = createSequentialWorkflowDef(workflowName, taskNames);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofMinutes(1));
        assertEquals("COMPLETED", status.getStatus());

        // Get activity types from Temporal history
        List<String> activityTypes = getActivityTypesFromHistory(workflowId);
        log.info("Activity types from history: {}", activityTypes);

        // Verify all activity types match task names
        assertEquals(taskNames.size(), activityTypes.size(),
                "Should have activity for each task");

        for (String taskName : taskNames) {
            assertTrue(activityTypes.contains(taskName),
                    "Activity type should include task: " + taskName);
        }
    }

    @Test
    void testActivityTypeNotGenericExecuteTask() throws Exception {
        String workflowName = "non_generic_activity_workflow";
        String taskName = "custom_task_name";

        // Register task definition
        registerTaskDefs(createTaskDefs(List.of(taskName)));

        // Register workflow definition
        WorkflowDef workflowDef = createSimpleWorkflowDef(workflowName, taskName);
        registerWorkflowDef(workflowDef);

        // Start workflow
        String workflowId = startWorkflow(workflowName, 1, Collections.emptyMap());

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofMinutes(1));
        assertEquals("COMPLETED", status.getStatus());

        // Get activity types
        List<String> activityTypes = getActivityTypesFromHistory(workflowId);

        // Verify activity is NOT using generic name
        for (String activityType : activityTypes) {
            assertTrue(!activityType.equals("executeTask") && !activityType.equals("TaskExecutionActivities"),
                    "Activity type should not be generic 'executeTask' or 'TaskExecutionActivities', " +
                            "but was: " + activityType);
        }

        // Verify activity is using the custom task name
        assertTrue(activityTypes.contains(taskName),
                "Activity type should be the custom task name: " + taskName);
    }
}
