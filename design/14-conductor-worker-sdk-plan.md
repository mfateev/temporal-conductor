# Temporal Conductor Java SDK - Implementation Plan

## Overview

Create a `temporal-conductor-java-sdk` module that allows existing Netflix Conductor worker implementations to run as Temporal activities **without any code changes**.

### Goals

1. **Zero code changes** for existing Conductor workers using `@WorkerTask` / `@InputParam`
2. **Same developer experience** as Conductor Java SDK
3. **Spring Boot auto-configuration** support
4. **Full compatibility** with Conductor worker patterns (return types, void, exceptions)

---

## Phase 1: Core SDK Module

### 1.1 Create Module Structure

```
temporal-conductor-java-sdk/
├── build.gradle
└── src/main/java/io/temporal/conductor/sdk/
    ├── worker/
    │   ├── ConductorWorkerRegistry.java      # Registry of task→method mappings
    │   ├── WorkerMethodInvoker.java          # Invokes methods with param mapping
    │   ├── WorkerMethodMetadata.java         # Metadata about a worker method
    │   └── WorkerDiscovery.java              # Scans for @WorkerTask methods
    ├── activity/
    │   └── WorkerTaskActivity.java           # Activity that delegates to workers
    └── spring/
        └── ConductorWorkerAutoConfiguration.java  # Spring Boot auto-config
```

**build.gradle:**
```gradle
plugins {
    id 'java-library'
}

dependencies {
    // Conductor SDK - for annotations (@WorkerTask, @InputParam)
    api 'org.conductoross:conductor-client:4.0.1'

    // Temporal SDK
    implementation 'io.temporal:temporal-sdk:1.25.0'

    // Reflection utilities
    implementation 'org.reflections:reflections:0.10.2'

    // Optional Spring Boot
    compileOnly 'org.springframework.boot:spring-boot-autoconfigure:3.2.0'

    // Jackson for serialization
    implementation 'com.fasterxml.jackson.core:jackson-databind'
}
```

### 1.2 ConductorWorkerRegistry

Core registry that maps task names to worker methods.

```java
package io.temporal.conductor.sdk.worker;

public class ConductorWorkerRegistry {

    private final Map<String, WorkerMethodMetadata> taskWorkers = new ConcurrentHashMap<>();

    /**
     * Register a worker instance. Scans for @WorkerTask annotated methods.
     */
    public void registerWorker(Object workerInstance) {
        for (Method method : workerInstance.getClass().getMethods()) {
            WorkerTask annotation = method.getAnnotation(WorkerTask.class);
            if (annotation != null) {
                String taskName = annotation.value();
                WorkerMethodMetadata metadata = new WorkerMethodMetadata(
                    workerInstance, method, annotation);
                taskWorkers.put(taskName, metadata);
                logger.info("Registered worker for task '{}': {}.{}()",
                    taskName, workerInstance.getClass().getSimpleName(), method.getName());
            }
        }
    }

    /**
     * Scan packages for @WorkerTask annotated methods.
     */
    public void scanPackages(String... packages) {
        Reflections reflections = new Reflections(packages);
        Set<Method> methods = reflections.getMethodsAnnotatedWith(WorkerTask.class);
        // ... instantiate and register
    }

    /**
     * Get worker metadata for a task name.
     */
    public Optional<WorkerMethodMetadata> getWorker(String taskName) {
        return Optional.ofNullable(taskWorkers.get(taskName));
    }

    /**
     * Check if a task has a registered worker.
     */
    public boolean hasWorker(String taskName) {
        return taskWorkers.containsKey(taskName);
    }

    /**
     * Get all registered task names.
     */
    public Set<String> getRegisteredTasks() {
        return Collections.unmodifiableSet(taskWorkers.keySet());
    }

    // Builder pattern
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final List<Object> instances = new ArrayList<>();
        private final List<String> packages = new ArrayList<>();

        public Builder addWorker(Object worker) {
            instances.add(worker);
            return this;
        }

        public Builder scanPackage(String packageName) {
            packages.add(packageName);
            return this;
        }

        public ConductorWorkerRegistry build() {
            ConductorWorkerRegistry registry = new ConductorWorkerRegistry();
            instances.forEach(registry::registerWorker);
            if (!packages.isEmpty()) {
                registry.scanPackages(packages.toArray(new String[0]));
            }
            return registry;
        }
    }
}
```

### 1.3 WorkerMethodMetadata

Stores metadata about a worker method for invocation.

```java
package io.temporal.conductor.sdk.worker;

public class WorkerMethodMetadata {
    private final Object instance;
    private final Method method;
    private final WorkerTask annotation;
    private final List<ParameterInfo> parameters;

    public WorkerMethodMetadata(Object instance, Method method, WorkerTask annotation) {
        this.instance = instance;
        this.method = method;
        this.annotation = annotation;
        this.parameters = extractParameters(method);
    }

    private List<ParameterInfo> extractParameters(Method method) {
        List<ParameterInfo> params = new ArrayList<>();
        Parameter[] methodParams = method.getParameters();

        for (int i = 0; i < methodParams.length; i++) {
            Parameter param = methodParams[i];
            InputParam inputParam = param.getAnnotation(InputParam.class);

            if (inputParam != null) {
                params.add(new ParameterInfo(
                    inputParam.value(),
                    param.getType(),
                    inputParam.required(),
                    i
                ));
            } else {
                // Support parameter name fallback (requires -parameters compiler flag)
                params.add(new ParameterInfo(
                    param.getName(),
                    param.getType(),
                    false,
                    i
                ));
            }
        }
        return params;
    }

    public String getTaskName() {
        return annotation.value();
    }

    public Object getInstance() {
        return instance;
    }

    public Method getMethod() {
        return method;
    }

    public List<ParameterInfo> getParameters() {
        return parameters;
    }

    public Class<?> getReturnType() {
        return method.getReturnType();
    }

    public record ParameterInfo(
        String name,
        Class<?> type,
        boolean required,
        int index
    ) {}
}
```

### 1.4 WorkerMethodInvoker

Invokes worker methods with proper parameter mapping.

```java
package io.temporal.conductor.sdk.worker;

public class WorkerMethodInvoker {

    private final ObjectMapper objectMapper;

    public WorkerMethodInvoker(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Invoke a worker method with the given input data.
     *
     * @param metadata Worker method metadata
     * @param inputData Task input data map
     * @return Result converted to Map for task output
     */
    public Map<String, Object> invoke(WorkerMethodMetadata metadata,
                                       Map<String, Object> inputData)
            throws Exception {

        Object[] args = buildArguments(metadata, inputData);
        Object result = metadata.getMethod().invoke(metadata.getInstance(), args);
        return convertResult(result, metadata.getReturnType());
    }

    private Object[] buildArguments(WorkerMethodMetadata metadata,
                                    Map<String, Object> inputData) {
        List<ParameterInfo> params = metadata.getParameters();
        Object[] args = new Object[params.size()];

        for (int i = 0; i < params.size(); i++) {
            ParameterInfo param = params.get(i);
            Object value = inputData.get(param.name());

            if (value == null && param.required()) {
                throw new IllegalArgumentException(
                    "Required parameter '" + param.name() + "' is missing");
            }

            // Convert value to expected type
            args[i] = convertValue(value, param.type());
        }

        return args;
    }

    private Object convertValue(Object value, Class<?> targetType) {
        if (value == null) {
            return null;
        }
        if (targetType.isInstance(value)) {
            return value;
        }
        // Use Jackson for complex type conversion
        return objectMapper.convertValue(value, targetType);
    }

    private Map<String, Object> convertResult(Object result, Class<?> returnType) {
        if (result == null || returnType == void.class || returnType == Void.class) {
            return Collections.emptyMap();
        }

        // If result is already a Map, return it
        if (result instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> mapResult = (Map<String, Object>) result;
            return mapResult;
        }

        // For primitives/strings, wrap in "result" key
        if (isPrimitiveOrWrapper(returnType) || returnType == String.class) {
            return Map.of("result", result);
        }

        // For objects, convert to Map
        return objectMapper.convertValue(result, new TypeReference<Map<String, Object>>() {});
    }

    private boolean isPrimitiveOrWrapper(Class<?> type) {
        return type.isPrimitive() ||
               type == Boolean.class || type == Integer.class ||
               type == Long.class || type == Double.class ||
               type == Float.class || type == Short.class ||
               type == Byte.class || type == Character.class;
    }
}
```

---

## Phase 2: Activity Integration

### 2.1 Update TaskExecutionActivitiesImpl

Modify to use the worker registry when available.

```java
package io.temporal.conductor.activity;

@Component
public class TaskExecutionActivitiesImpl implements DynamicActivity {

    private final ConductorWorkerRegistry workerRegistry;
    private final WorkerMethodInvoker invoker;
    private final ExtensionTaskExecutionActivityImpl extensionActivity;

    @Autowired
    public TaskExecutionActivitiesImpl(
            @Autowired(required = false) ConductorWorkerRegistry workerRegistry,
            ExtensionTaskExecutionActivityImpl extensionActivity,
            ObjectMapper objectMapper) {
        this.workerRegistry = workerRegistry;
        this.extensionActivity = extensionActivity;
        this.invoker = new WorkerMethodInvoker(objectMapper);
    }

    @Override
    public Object execute(EncodedValues args) {
        String activityType = Activity.getExecutionContext().getInfo().getActivityType();
        String taskRefName = args.get(0, String.class);
        String conductorTaskType = args.get(1, String.class);
        Map<String, Object> input = args.get(2, Map.class);

        logger.info("Executing task: {} (ref: {}, type: {})",
            activityType, taskRefName, conductorTaskType);

        // 1. Check for extension task types (JSON_JQ_TRANSFORM, HTTP, etc.)
        if (EXTENSION_TASK_TYPES.contains(conductorTaskType)) {
            return executeExtensionTask(conductorTaskType, taskRefName, input);
        }

        // 2. Check for registered worker (NEW!)
        if (workerRegistry != null && workerRegistry.hasWorker(activityType)) {
            return executeRegisteredWorker(activityType, taskRefName, input);
        }

        // 3. Fall back to stub implementation
        return executeStubTask(activityType, taskRefName, input);
    }

    private TaskExecutionResult executeRegisteredWorker(
            String taskName, String taskRefName, Map<String, Object> input) {

        WorkerMethodMetadata metadata = workerRegistry.getWorker(taskName)
            .orElseThrow(() -> new IllegalStateException(
                "Worker not found for task: " + taskName));

        try {
            Map<String, Object> output = invoker.invoke(metadata, input);
            logger.info("Worker task completed: {} → {}", taskRefName, output);
            return TaskExecutionResult.builder()
                .output(output)
                .status("COMPLETED")
                .build();

        } catch (Exception e) {
            logger.error("Worker task failed: {}", taskRefName, e);
            return TaskExecutionResult.builder()
                .output(Collections.emptyMap())
                .status("FAILED")
                .failureReason(e.getMessage())
                .build();
        }
    }

    // ... existing methods for extension and stub tasks
}
```

---

## Phase 3: Spring Boot Auto-Configuration

### 3.1 ConductorWorkerAutoConfiguration

Auto-discover workers from Spring context.

```java
package io.temporal.conductor.sdk.spring;

@AutoConfiguration
@ConditionalOnClass(WorkerTask.class)
public class ConductorWorkerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ConductorWorkerRegistry conductorWorkerRegistry(
            ApplicationContext context,
            @Value("${conductor.worker.packages:}") String packages) {

        ConductorWorkerRegistry.Builder builder = ConductorWorkerRegistry.builder();

        // Scan configured packages
        if (!packages.isBlank()) {
            for (String pkg : packages.split(",")) {
                builder.scanPackage(pkg.trim());
            }
        }

        // Auto-discover from Spring beans with @WorkerTask methods
        Map<String, Object> beans = context.getBeansWithAnnotation(Component.class);
        for (Object bean : beans.values()) {
            if (hasWorkerTaskMethods(bean)) {
                builder.addWorker(bean);
            }
        }

        ConductorWorkerRegistry registry = builder.build();
        logger.info("Registered {} Conductor workers as Temporal activities: {}",
            registry.getRegisteredTasks().size(), registry.getRegisteredTasks());

        return registry;
    }

    private boolean hasWorkerTaskMethods(Object bean) {
        return Arrays.stream(bean.getClass().getMethods())
            .anyMatch(m -> m.isAnnotationPresent(WorkerTask.class));
    }
}
```

### 3.2 Configuration Properties

```java
package io.temporal.conductor.sdk.spring;

@ConfigurationProperties(prefix = "conductor.worker")
public class ConductorWorkerProperties {

    /**
     * Comma-separated list of packages to scan for @WorkerTask methods.
     */
    private String packages = "";

    /**
     * Whether to auto-discover workers from Spring beans.
     */
    private boolean autoDiscover = true;

    // getters/setters
}
```

**application.yml:**
```yaml
conductor:
  worker:
    packages: com.myapp.workers
    auto-discover: true
```

---

## Phase 4: Testing

### 4.1 Unit Tests

```java
class ConductorWorkerRegistryTest {

    @Test
    void testRegisterWorker() {
        ConductorWorkerRegistry registry = new ConductorWorkerRegistry();
        registry.registerWorker(new TestWorkers());

        assertTrue(registry.hasWorker("greet"));
        assertTrue(registry.hasWorker("get_user"));
        assertFalse(registry.hasWorker("unknown"));
    }

    @Test
    void testWorkerInvocation() {
        ConductorWorkerRegistry registry = ConductorWorkerRegistry.builder()
            .addWorker(new TestWorkers())
            .build();

        WorkerMethodInvoker invoker = new WorkerMethodInvoker(new ObjectMapper());
        WorkerMethodMetadata metadata = registry.getWorker("greet").orElseThrow();

        Map<String, Object> input = Map.of("name", "World");
        Map<String, Object> output = invoker.invoke(metadata, input);

        assertEquals("Hello World", output.get("result"));
    }

    static class TestWorkers {
        @WorkerTask("greet")
        public String greet(@InputParam("name") String name) {
            return "Hello " + name;
        }

        @WorkerTask("get_user")
        public UserInfo getUser(@InputParam("id") String id) {
            return new UserInfo(id, "Test User");
        }
    }
}
```

### 4.2 Integration Tests

```java
class WorkerTaskActivityIntegrationTest {

    private TestWorkflowEnvironment testEnv;

    @BeforeEach
    void setUp() {
        testEnv = TestWorkflowEnvironment.newInstance();

        // Register workers
        ConductorWorkerRegistry registry = ConductorWorkerRegistry.builder()
            .addWorker(new GreetingWorkers())
            .build();

        Worker worker = testEnv.newWorker(TASK_QUEUE);
        worker.registerWorkflowImplementationTypes(ConductorWorkflowImpl.class);
        worker.registerActivitiesImplementations(
            new TaskExecutionActivitiesImpl(registry, null, new ObjectMapper()));

        testEnv.start();
    }

    @Test
    void testWorkerExecutesInWorkflow() {
        // Create workflow with SIMPLE task "greet"
        WorkflowDef workflowDef = createWorkflowWithTask("greet");

        // Start workflow
        ConductorWorkflow workflow = client.newWorkflowStub(ConductorWorkflow.class);
        ConductorWorkflowOutput output = workflow.execute(
            new ConductorWorkflowInput(workflowDef, Map.of("name", "Temporal")));

        // Verify worker was invoked
        assertEquals("COMPLETED", output.getStatus());
        assertEquals("Hello Temporal",
            output.getTasks().get(0).getOutputData().get("result"));
    }

    static class GreetingWorkers {
        @WorkerTask("greet")
        public String greet(@InputParam("name") String name) {
            return "Hello " + name;
        }
    }
}
```

### 4.3 E2E Tests

```java
class WorkerTaskE2ETest extends AbstractE2ETest {

    @Test
    void testExistingConductorWorkerRunsAsTemporalActivity() {
        // Register workflow
        WorkflowDef workflowDef = createSimpleWorkflowDef("worker_test", "greet");
        registerWorkflowDef(workflowDef);

        // Start workflow with input
        String workflowId = startWorkflow("worker_test", 1,
            Map.of("name", "E2E Test"));

        // Wait for completion
        WorkflowStatusResponse status = waitForWorkflowCompletion(
            workflowId, Duration.ofSeconds(30));

        assertEquals("COMPLETED", status.getStatus());

        // Verify worker output
        var greetTask = status.getTasks().stream()
            .filter(t -> "greet_ref".equals(t.getReferenceTaskName()))
            .findFirst().orElseThrow();

        assertEquals("Hello E2E Test", greetTask.getOutputData().get("result"));
    }
}
```

---

## Phase 5: Documentation & Examples

### 5.1 README.md for SDK

```markdown
# Temporal Conductor Java SDK

Run existing Netflix Conductor workers as Temporal activities without code changes.

## Quick Start

### 1. Add Dependency

```gradle
dependencies {
    implementation 'io.temporal:temporal-conductor-java-sdk:1.0.0'
}
```

### 2. Use Existing Conductor Workers

```java
// Your existing Conductor workers - NO CHANGES NEEDED
public class MyWorkers {

    @WorkerTask("greet")
    public String greet(@InputParam("name") String name) {
        return "Hello " + name;
    }
}
```

### 3. Register with Temporal

```java
// Option A: Programmatic
ConductorWorkerRegistry registry = ConductorWorkerRegistry.builder()
    .addWorker(new MyWorkers())
    .build();

Worker worker = factory.newWorker(taskQueue);
worker.registerActivitiesImplementations(
    new TaskExecutionActivitiesImpl(registry));

// Option B: Spring Boot (auto-discovery)
@Component  // Just add to Spring context
public class MyWorkers { ... }
```
```

### 5.2 Update DEMO.md

Add section for running custom workers.

---

## Implementation Order

| Phase | Component | Effort | Priority |
|-------|-----------|--------|----------|
| 1.1 | Module structure & build.gradle | S | P0 |
| 1.2 | ConductorWorkerRegistry | M | P0 |
| 1.3 | WorkerMethodMetadata | S | P0 |
| 1.4 | WorkerMethodInvoker | M | P0 |
| 2.1 | Update TaskExecutionActivitiesImpl | M | P0 |
| 4.1 | Unit tests | M | P0 |
| 4.2 | Integration tests | M | P1 |
| 3.1 | Spring auto-configuration | S | P1 |
| 4.3 | E2E tests | M | P1 |
| 5.1 | SDK documentation | S | P2 |
| 5.2 | Update DEMO.md | S | P2 |

**Total Effort**: ~2-3 days

---

## Open Questions

1. **Conductor SDK dependency**: Should we depend on `conductor-client` for annotations, or copy the annotation classes to avoid the dependency?

2. **Worker interface support**: Should we also support the `Worker` interface pattern, or only annotations?

3. **Error handling**: How should worker exceptions map to Temporal activity failures? Retryable vs non-retryable?

4. **Async workers**: Should we support async/CompletableFuture return types?

---

## Success Criteria

1. Existing `@WorkerTask` annotated methods work without code changes
2. Spring Boot auto-discovery works out of the box
3. E2E test passes with real worker execution
4. No performance regression vs stub implementation
