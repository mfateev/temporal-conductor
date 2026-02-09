# Temporal Conductor

Execute [Netflix Conductor](https://conductor-oss.github.io/conductor/) workflow definitions using [Temporal](https://temporal.io/) as the execution backend.

## Overview

Temporal Conductor interprets Conductor workflow definitions at runtime using a single Temporal workflow—no code generation required. It reuses Conductor's core libraries (DeciderService, expression evaluation, task mappers) with in-memory DAO implementations, while Temporal provides durability via event history.

### Key Benefits

- **No database required** - Temporal's event history replaces Redis/PostgreSQL
- **Familiar workflow format** - Use existing Conductor JSON workflow definitions
- **Production-grade execution** - Temporal handles retries, timeouts, and fault tolerance
- **REST API compatible** - Conductor-compatible REST endpoints for workflow management

## Supported Task Types

| Task Type | Status | Description |
|-----------|--------|-------------|
| SIMPLE | ✅ | Worker tasks with real processing (compute, transform, validate) |
| HTTP | ✅ | HTTP request tasks |
| FORK_JOIN | ✅ | Parallel execution branches |
| JOIN | ✅ | Wait for parallel branches |
| SWITCH/DECISION | ✅ | Conditional branching |
| DO_WHILE | ✅ | Loop execution with continue-as-new for large iterations |
| SET_VARIABLE | ✅ | Set workflow variables |
| TERMINATE | ✅ | Terminate workflow execution |
| WAIT | ✅ | Wait for signal or timeout |
| HUMAN | ✅ | Human task with signal completion |
| EVENT | ✅ | Publish events to queues |
| SUB_WORKFLOW | ✅ | Child workflow execution |
| DYNAMIC | ✅ | Dynamic task type resolution |
| JSON_JQ_TRANSFORM | ✅ | JQ-based JSON transformation |

## Quick Start

### Prerequisites

- JDK 21+
- Docker and Docker Compose

### Using Docker Compose (Recommended)

```bash
# Start Temporal + Conductor server
docker compose -f docker/docker-compose.yml up -d

# Wait for initialization
docker compose -f docker/docker-compose.yml logs init-temporal -f

# Services:
# - Conductor UI:     http://localhost:5001
# - Conductor API:    http://localhost:8080
# - Swagger UI:       http://localhost:8080/swagger-ui.html
# - Temporal UI:      http://localhost:8234
```

### Local Development

```bash
# Terminal 1: Start Temporal dev server
./scripts/start-temporal.sh

# Terminal 2: Start Conductor server
SPRING_PROFILES_ACTIVE=temporal ./gradlew :temporal-conductor:bootRun
```

## Usage

### Register a Workflow

```bash
# Register task definition
curl -X POST http://localhost:8080/api/metadata/taskdefs \
  -H "Content-Type: application/json" \
  -d '[{"name": "greet", "timeoutSeconds": 0, "responseTimeoutSeconds": 0}]'

# Register workflow definition
curl -X POST http://localhost:8080/api/metadata/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "hello_workflow",
    "version": 1,
    "tasks": [
      {
        "name": "greet",
        "taskReferenceName": "greet_task",
        "type": "SIMPLE",
        "inputParameters": {
          "name": "${workflow.input.userName}"
        }
      }
    ]
  }'
```

### Start a Workflow

```bash
curl -X POST "http://localhost:8080/api/workflow" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "hello_workflow",
    "version": 1,
    "input": {"userName": "World"}
  }'
```

### Check Workflow Status

```bash
curl http://localhost:8080/api/workflow/{workflowId}
```

## Architecture

```
┌─────────────────────────────────────────────────┐
│  REST API Layer (Spring Boot)                   │
│  - WorkflowResource, TaskResource, etc.         │
└─────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│  Temporal Workflow (ConductorWorkflowImpl)      │
│  - DeciderService (Conductor library)           │
│  - SystemTaskExecutor                           │
│  - In-Memory DAOs                               │
└─────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│  Temporal Activities                            │
│  - TaskExecutionActivities (SIMPLE/HTTP)        │
│  - ExtensionTaskExecutionActivity (JQ, etc.)    │
│  - EventPublishActivity                         │
└─────────────────────────────────────────────────┘
```

## Project Structure

```
temporal-conductor/
├── temporal-conductor/       # Main module
│   └── src/main/java/io/temporal/conductor/
│       ├── workflow/         # Temporal workflow implementation
│       ├── executor/         # DeciderService, SystemTaskExecutor
│       ├── activity/         # Temporal activities
│       ├── api/              # REST endpoints
│       ├── service/          # Service layer
│       └── config/           # Spring configuration
├── e2e-tests/                # End-to-end tests
├── design/                   # Design documents
└── docker/                   # Docker configurations
```

## Building

```bash
# Build and test
./gradlew build

# Run tests only
./gradlew test

# Run specific tests
./gradlew test --tests "*WorkflowTest*"

# Run E2E tests (requires Docker)
./gradlew :e2e-tests:test
```

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `TEMPORAL_ADDRESS` | `localhost:7234` | Temporal server gRPC address |
| `TEMPORAL_NAMESPACE` | `conductor` | Temporal namespace |
| `TEMPORAL_TASK_QUEUE` | `conductor-workflows` | Worker task queue |

## Documentation

- [DEMO.md](DEMO.md) - Detailed demo guide with examples
- [DESIGN.md](DESIGN.md) - Design overview and decisions
- [design/](design/) - Detailed design documents

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Netflix Conductor](https://github.com/conductor-oss/conductor) - Workflow orchestration engine
- [Temporal](https://temporal.io/) - Durable execution platform
