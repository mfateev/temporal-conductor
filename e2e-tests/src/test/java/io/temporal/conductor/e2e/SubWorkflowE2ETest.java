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

import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.SubWorkflowParams;
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
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * E2E tests for SUB_WORKFLOW task type against real Temporal server.
 */
class SubWorkflowE2ETest extends AbstractE2ETest {

    private static final Logger log = LoggerFactory.getLogger(SubWorkflowE2ETest.class);

    @Test
    void testSimpleSubWorkflow() throws Exception {
        String parentWorkflowName = "parent_sub_workflow_e2e";
        String childWorkflowName = "child_sub_workflow_e2e";
        String childTaskName = "child_task";

        // Register child workflow's task
        registerTaskDefs(createTaskDefs(List.of(childTaskName)));

        // Register child workflow
        WorkflowDef childWorkflowDef = createSimpleWorkflowDef(childWorkflowName, childTaskName);
        registerWorkflowDef(childWorkflowDef);

        // Register parent workflow with SUB_WORKFLOW task
        WorkflowDef parentWorkflowDef = createParentWorkflowWithSubWorkflow(
                parentWorkflowName, childWorkflowName, 1);
        registerWorkflowDef(parentWorkflowDef);

        // Start parent workflow
        String workflowId = startWorkflow(parentWorkflowName, 1, Collections.emptyMap());
        log.info("Started parent workflow: {}", workflowId);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus());

        // Verify SUB_WORKFLOW task completed
        assertNotNull(status.getTasks());
        Set<String> completedTaskRefs = status.getTasks().stream()
                .filter(t -> "COMPLETED".equals(t.getStatus()))
                .map(WorkflowStatusResponse.TaskInfo::getReferenceTaskName)
                .collect(Collectors.toSet());

        log.info("Completed task references: {}", completedTaskRefs);
        assertTrue(completedTaskRefs.contains("sub_workflow_ref"),
                "SUB_WORKFLOW task should be completed");
    }

    @Test
    void testSubWorkflowWithInlineDefinition() throws Exception {
        String parentWorkflowName = "parent_inline_sub_e2e";
        String childTaskName = "inline_child_task";

        // Register child workflow's task
        registerTaskDefs(createTaskDefs(List.of(childTaskName)));

        // Register parent workflow with inline SUB_WORKFLOW definition
        WorkflowDef parentWorkflowDef = createParentWorkflowWithInlineSubWorkflow(
                parentWorkflowName, childTaskName);
        registerWorkflowDef(parentWorkflowDef);

        // Start parent workflow
        String workflowId = startWorkflow(parentWorkflowName, 1, Collections.emptyMap());
        log.info("Started parent workflow with inline sub-workflow: {}", workflowId);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(60));
        assertEquals("COMPLETED", status.getStatus());

        // Verify SUB_WORKFLOW task completed
        Set<String> completedTaskRefs = status.getTasks().stream()
                .filter(t -> "COMPLETED".equals(t.getStatus()))
                .map(WorkflowStatusResponse.TaskInfo::getReferenceTaskName)
                .collect(Collectors.toSet());

        assertTrue(completedTaskRefs.contains("inline_sub_workflow_ref"),
                "Inline SUB_WORKFLOW task should be completed");
    }

    @Test
    void testNestedSubWorkflows() throws Exception {
        // Test: Parent -> Child -> Grandchild (3 levels of nesting)
        String parentName = "nested_parent_e2e";
        String childName = "nested_child_e2e";
        String grandchildName = "nested_grandchild_e2e";
        String leafTaskName = "leaf_task";

        // Register leaf task
        registerTaskDefs(createTaskDefs(List.of(leafTaskName)));

        // Register grandchild workflow (leaf level)
        WorkflowDef grandchildDef = createSimpleWorkflowDef(grandchildName, leafTaskName);
        registerWorkflowDef(grandchildDef);

        // Register child workflow that calls grandchild
        WorkflowDef childDef = createParentWorkflowWithSubWorkflow(childName, grandchildName, 1);
        registerWorkflowDef(childDef);

        // Register parent workflow that calls child
        WorkflowDef parentDef = createParentWorkflowWithSubWorkflow(parentName, childName, 1);
        registerWorkflowDef(parentDef);

        // Start parent workflow
        String workflowId = startWorkflow(parentName, 1, Collections.emptyMap());
        log.info("Started nested workflow hierarchy: {}", workflowId);

        // Wait for completion (longer timeout for nested workflows)
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(90));
        assertEquals("COMPLETED", status.getStatus());

        log.info("Nested workflow completed successfully");
    }

    @Test
    void testSubWorkflowInParallel() throws Exception {
        // Test: FORK_JOIN with multiple SUB_WORKFLOW tasks in parallel branches
        String parentName = "parallel_sub_workflow_e2e";
        String child1Name = "parallel_child_1_e2e";
        String child2Name = "parallel_child_2_e2e";
        String childTaskName = "parallel_child_task";
        String finalTaskName = "final_after_parallel";

        // Register tasks
        registerTaskDefs(createTaskDefs(List.of(childTaskName, finalTaskName)));

        // Register child workflows
        WorkflowDef child1Def = createSimpleWorkflowDef(child1Name, childTaskName);
        WorkflowDef child2Def = createSimpleWorkflowDef(child2Name, childTaskName);
        registerWorkflowDef(child1Def);
        registerWorkflowDef(child2Def);

        // Register parent workflow with FORK_JOIN containing SUB_WORKFLOW tasks
        WorkflowDef parentDef = createForkJoinWithSubWorkflows(
                parentName, child1Name, child2Name, finalTaskName);
        registerWorkflowDef(parentDef);

        // Start parent workflow
        String workflowId = startWorkflow(parentName, 1, Collections.emptyMap());
        log.info("Started parallel sub-workflow test: {}", workflowId);

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(workflowId, Duration.ofSeconds(90));
        assertEquals("COMPLETED", status.getStatus());

        // Verify all tasks completed
        Set<String> completedTaskRefs = status.getTasks().stream()
                .filter(t -> "COMPLETED".equals(t.getStatus()))
                .map(WorkflowStatusResponse.TaskInfo::getReferenceTaskName)
                .collect(Collectors.toSet());

        log.info("Completed tasks in parallel sub-workflow: {}", completedTaskRefs);
        assertTrue(completedTaskRefs.contains("sub_workflow_1_ref"),
                "First SUB_WORKFLOW should be completed");
        assertTrue(completedTaskRefs.contains("sub_workflow_2_ref"),
                "Second SUB_WORKFLOW should be completed");
        assertTrue(completedTaskRefs.contains(finalTaskName + "_ref"),
                "Final task after JOIN should be completed");
    }

    // ==================== Helper Methods ====================

    /**
     * Create a parent workflow with a SUB_WORKFLOW task that calls another workflow by name.
     */
    private WorkflowDef createParentWorkflowWithSubWorkflow(String parentName, String childWorkflowName, int childVersion) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(parentName);
        workflowDef.setVersion(1);

        WorkflowTask subWorkflowTask = new WorkflowTask();
        subWorkflowTask.setName("sub_workflow_task");
        subWorkflowTask.setTaskReferenceName("sub_workflow_ref");
        subWorkflowTask.setType(TaskType.SUB_WORKFLOW.name());

        SubWorkflowParams subParams = new SubWorkflowParams();
        subParams.setName(childWorkflowName);
        subParams.setVersion(childVersion);
        subWorkflowTask.setSubWorkflowParam(subParams);

        Map<String, Object> inputParams = new HashMap<>();
        inputParams.put("workflowInput", Collections.emptyMap());
        subWorkflowTask.setInputParameters(inputParams);

        workflowDef.setTasks(Collections.singletonList(subWorkflowTask));
        return workflowDef;
    }

    /**
     * Create a parent workflow with an inline SUB_WORKFLOW definition.
     */
    private WorkflowDef createParentWorkflowWithInlineSubWorkflow(String parentName, String childTaskName) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(parentName);
        workflowDef.setVersion(1);

        // Create inline child workflow definition
        WorkflowDef inlineChildDef = new WorkflowDef();
        inlineChildDef.setName("inline_child");
        inlineChildDef.setVersion(1);

        WorkflowTask childTask = new WorkflowTask();
        childTask.setName(childTaskName);
        childTask.setTaskReferenceName(childTaskName + "_ref");
        childTask.setType(TaskType.SIMPLE.name());
        inlineChildDef.setTasks(Collections.singletonList(childTask));

        // Create SUB_WORKFLOW task with inline definition
        WorkflowTask subWorkflowTask = new WorkflowTask();
        subWorkflowTask.setName("inline_sub_workflow");
        subWorkflowTask.setTaskReferenceName("inline_sub_workflow_ref");
        subWorkflowTask.setType(TaskType.SUB_WORKFLOW.name());

        SubWorkflowParams subParams = new SubWorkflowParams();
        subParams.setWorkflowDefinition(inlineChildDef);
        subWorkflowTask.setSubWorkflowParam(subParams);

        Map<String, Object> inputParams = new HashMap<>();
        inputParams.put("workflowInput", Collections.emptyMap());
        subWorkflowTask.setInputParameters(inputParams);

        workflowDef.setTasks(Collections.singletonList(subWorkflowTask));
        return workflowDef;
    }

    /**
     * Create a workflow with FORK_JOIN containing SUB_WORKFLOW tasks in parallel branches.
     */
    private WorkflowDef createForkJoinWithSubWorkflows(String parentName, String child1Name, String child2Name, String finalTaskName) {
        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(parentName);
        workflowDef.setVersion(1);

        List<WorkflowTask> tasks = new ArrayList<>();

        // FORK task
        WorkflowTask forkTask = new WorkflowTask();
        forkTask.setName("fork_ref");
        forkTask.setTaskReferenceName("fork_ref");
        forkTask.setType(TaskType.FORK_JOIN.name());

        List<List<WorkflowTask>> forkTasks = new ArrayList<>();
        List<String> joinOn = new ArrayList<>();

        // Branch 1: SUB_WORKFLOW calling child1
        WorkflowTask sub1 = new WorkflowTask();
        sub1.setName("sub_workflow_1");
        sub1.setTaskReferenceName("sub_workflow_1_ref");
        sub1.setType(TaskType.SUB_WORKFLOW.name());
        SubWorkflowParams params1 = new SubWorkflowParams();
        params1.setName(child1Name);
        params1.setVersion(1);
        sub1.setSubWorkflowParam(params1);
        Map<String, Object> input1 = new HashMap<>();
        input1.put("workflowInput", Collections.emptyMap());
        sub1.setInputParameters(input1);
        forkTasks.add(Collections.singletonList(sub1));
        joinOn.add("sub_workflow_1_ref");

        // Branch 2: SUB_WORKFLOW calling child2
        WorkflowTask sub2 = new WorkflowTask();
        sub2.setName("sub_workflow_2");
        sub2.setTaskReferenceName("sub_workflow_2_ref");
        sub2.setType(TaskType.SUB_WORKFLOW.name());
        SubWorkflowParams params2 = new SubWorkflowParams();
        params2.setName(child2Name);
        params2.setVersion(1);
        sub2.setSubWorkflowParam(params2);
        Map<String, Object> input2 = new HashMap<>();
        input2.put("workflowInput", Collections.emptyMap());
        sub2.setInputParameters(input2);
        forkTasks.add(Collections.singletonList(sub2));
        joinOn.add("sub_workflow_2_ref");

        forkTask.setForkTasks(forkTasks);
        tasks.add(forkTask);

        // JOIN task
        WorkflowTask joinTask = new WorkflowTask();
        joinTask.setName("join_ref");
        joinTask.setTaskReferenceName("join_ref");
        joinTask.setType(TaskType.JOIN.name());
        joinTask.setJoinOn(joinOn);
        tasks.add(joinTask);

        // Final task after join
        WorkflowTask finalTask = new WorkflowTask();
        finalTask.setName(finalTaskName);
        finalTask.setTaskReferenceName(finalTaskName + "_ref");
        finalTask.setType(TaskType.SIMPLE.name());
        tasks.add(finalTask);

        workflowDef.setTasks(tasks);
        return workflowDef;
    }
}
