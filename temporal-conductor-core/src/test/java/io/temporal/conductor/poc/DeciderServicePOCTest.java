package io.temporal.conductor.poc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.Task.Status;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.execution.DeciderService;
import com.netflix.conductor.core.execution.mapper.SimpleTaskMapper;
import com.netflix.conductor.core.execution.mapper.TaskMapper;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * POC Test 1: Validate that DeciderService can run standalone without Spring context.
 *
 * <p>Success Criteria: - DeciderService instantiates without Spring - decide() returns
 * DeciderOutcome with correct tasks - Sequential task execution works - Workflow reaches terminal
 * state
 */
public class DeciderServicePOCTest {
  private static final Logger logger = LoggerFactory.getLogger(DeciderServicePOCTest.class);

  private InMemoryMetadataDAO metadataDAO;
  private InMemoryExecutionDAO executionDAO;
  private DeciderService deciderService;

  @Before
  public void setup() {
    logger.info("=== Setting up POC Test ===");

    // Create in-memory DAOs
    metadataDAO = new InMemoryMetadataDAO();
    executionDAO = new InMemoryExecutionDAO();

    // Register test workflow and tasks
    metadataDAO.registerSimpleTask("task1");
    metadataDAO.registerSimpleTask("task2");
    metadataDAO.registerSimpleWorkflow("simple-test", "task1", "task2");

    // Create minimal dependencies for DeciderService
    MinimalIDGenerator idGenerator = new MinimalIDGenerator();
    ObjectMapper objectMapper = new ObjectMapper();
    MinimalParametersUtils parametersUtils = new MinimalParametersUtils(objectMapper);
    NoOpExternalPayloadStorage externalStorage = new NoOpExternalPayloadStorage();
    MinimalExternalPayloadStorageUtils externalPayloadStorageUtils =
        new MinimalExternalPayloadStorageUtils(externalStorage);
    MinimalSystemTaskRegistry systemTaskRegistry = new MinimalSystemTaskRegistry();

    // Create task mappers (just SIMPLE for POC)
    Map<String, TaskMapper> taskMappers = new HashMap<>();
    taskMappers.put(TaskType.SIMPLE.name(), new SimpleTaskMapper(parametersUtils));

    // Instantiate DeciderService
    deciderService =
        new DeciderService(
            idGenerator,
            parametersUtils,
            metadataDAO,
            externalPayloadStorageUtils,
            systemTaskRegistry,
            taskMappers,
            Duration.ofMinutes(60));

    logger.info("✓ DeciderService instantiated successfully without Spring!");
    logger.info("Setup complete");
  }

  @Test
  public void testSimpleSequentialWorkflow() {
    logger.info("=== Test: Simple Sequential Workflow ===");

    // Create workflow instance
    String workflowId = "test-wf-" + UUID.randomUUID();
    WorkflowDef workflowDef = metadataDAO.getLatestWorkflowDef("simple-test").orElseThrow();

    Workflow workflow = new Workflow();
    workflow.setWorkflowId(workflowId);
    workflow.setWorkflowDefinition(workflowDef);
    workflow.setStatus(Workflow.WorkflowStatus.RUNNING);
    workflow.setInput(new HashMap<>());

    executionDAO.createWorkflow(workflow);

    // TODO: Call DeciderService.decide() to get first task
    // DeciderOutcome outcome = deciderService.decide(workflowModel);

    // Verify: First task should be task1
    // assertNotNull(outcome.tasksToBeScheduled);
    // assertEquals(1, outcome.tasksToBeScheduled.size());
    // assertEquals("task1", outcome.tasksToBeScheduled.get(0).getTaskDefName());

    // TODO: Mark task1 as completed
    // task1.setStatus(Status.COMPLETED);
    // executionDAO.updateTask(task1);

    // TODO: Call decide() again to get next task
    // outcome = deciderService.decide(workflowModel);

    // Verify: Second task should be task2
    // assertEquals("task2", outcome.tasksToBeScheduled.get(0).getTaskDefName());

    // TODO: Mark task2 as completed
    // task2.setStatus(Status.COMPLETED);
    // executionDAO.updateTask(task2);

    // TODO: Call decide() again
    // outcome = deciderService.decide(workflowModel);

    // Verify: Workflow should be complete
    // assertTrue(workflowModel.isTerminalState());
    // assertEquals(Workflow.WorkflowStatus.COMPLETED, workflowModel.getStatus());

    logger.info("✓ POC Test 1 would validate here (DeciderService not yet instantiated)");
  }

  @Test
  public void testMetadataDAOBasics() {
    logger.info("=== Test: Metadata DAO Basics ===");

    // Verify task definitions were registered
    assertNotNull("task1 should be registered", metadataDAO.getTaskDef("task1"));
    assertNotNull("task2 should be registered", metadataDAO.getTaskDef("task2"));

    // Verify workflow definition was registered
    assertTrue(
        "Workflow should be registered", metadataDAO.getLatestWorkflowDef("simple-test").isPresent());

    WorkflowDef def = metadataDAO.getLatestWorkflowDef("simple-test").get();
    assertEquals("simple-test", def.getName());
    assertEquals(2, def.getTasks().size());
    assertEquals("task1", def.getTasks().get(0).getName());
    assertEquals("task2", def.getTasks().get(1).getName());

    logger.info("✓ Metadata DAO working correctly");
  }

  @Test
  public void testExecutionDAOBasics() {
    logger.info("=== Test: Execution DAO Basics ===");

    // Create a test workflow
    Workflow workflow = new Workflow();
    workflow.setWorkflowId("test-" + UUID.randomUUID());
    workflow.setWorkflowName("simple-test");
    workflow.setStatus(Workflow.WorkflowStatus.RUNNING);

    executionDAO.createWorkflow(workflow);

    // Retrieve and verify
    Workflow retrieved = executionDAO.getWorkflow(workflow.getWorkflowId());
    assertNotNull("Workflow should be retrievable", retrieved);
    assertEquals(workflow.getWorkflowId(), retrieved.getWorkflowId());
    assertEquals(Workflow.WorkflowStatus.RUNNING, retrieved.getStatus());

    // Create tasks
    Task task1 = new Task();
    task1.setTaskId(UUID.randomUUID().toString());
    task1.setTaskDefName("task1");
    task1.setWorkflowInstanceId(workflow.getWorkflowId());
    task1.setStatus(Status.SCHEDULED);

    executionDAO.createTasks(List.of(task1));

    // Retrieve task
    Task retrievedTask = executionDAO.getTask(task1.getTaskId());
    assertNotNull("Task should be retrievable", retrievedTask);
    assertEquals(task1.getTaskId(), retrievedTask.getTaskId());

    // Get workflow with tasks
    Workflow withTasks = executionDAO.getWorkflow(workflow.getWorkflowId(), true);
    assertNotNull(withTasks.getTasks());
    assertEquals(1, withTasks.getTasks().size());

    logger.info("✓ Execution DAO working correctly");
  }
}
