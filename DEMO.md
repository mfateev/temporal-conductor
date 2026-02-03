# Temporal Conductor Demo

This guide demonstrates how to use Conductor workflows executed on Temporal.

## Docker Compose (Recommended)

Start all services with a single command:

```bash
# Start Temporal + Conductor server
docker compose -f docker/docker-compose.yml up -d

# Wait for initialization (registers search attributes)
docker compose -f docker/docker-compose.yml logs init-temporal -f

# Services will be ready at:
# - Conductor UI:     http://localhost:5001
# - Conductor API:    http://localhost:8080
# - Swagger UI:       http://localhost:8080/swagger-ui.html
# - Temporal UI:      http://localhost:8234
```

### Stop Services

```bash
docker compose -f docker/docker-compose.yml down        # Stop services
docker compose -f docker/docker-compose.yml down -v     # Stop and remove volumes (clean state)
```

---

## Local Development (Alternative)

For development without Docker:

### Terminal 1: Start Temporal Server

```bash
# Start Temporal dev server with all required search attributes
./scripts/start-temporal.sh
```

### Terminal 2: Start Conductor Server

```bash
# Start the Conductor server with Temporal profile
SPRING_PROFILES_ACTIVE=temporal ./gradlew :temporal-conductor:bootRun
```

### Service URLs

| Service | URL | Purpose |
|---------|-----|---------|
| **Swagger UI** | http://localhost:8080/swagger-ui.html | REST API - start/monitor workflows |
| **Temporal UI** | http://localhost:8233 | View Temporal execution details |

---

## Running Demo Workflows

### Step 1: Register a Demo Workflow

Use Swagger UI (http://localhost:8080/swagger-ui.html) or curl to register:

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
    ],
    "outputParameters": {
      "greeting": "${greet_task.output.message}"
    }
  }'
```

### Step 2: Start Workflow via curl or Swagger

**Using curl:**
```bash
curl -X POST "http://localhost:8080/api/workflow" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "hello_workflow",
    "version": 1,
    "input": {"userName": "Alice"}
  }'
```

**Using Swagger UI:**
1. Open http://localhost:8080/swagger-ui.html
2. Find `POST /api/workflow`
3. Enter the request body and execute

### Step 3: View Progress in Temporal UI

**Temporal UI (http://localhost:8233):**
- Go to **Workflows** tab
- Filter by namespace `conductor`
- Click on the workflow to see:
  - Event history (detailed execution log)
  - Search attributes (ConductorWorkflowType, ConductorStatus)
  - Pending activities

---

## Running Demo Workflows (curl)

```bash
# Simple sequential workflow
curl -X POST "http://localhost:8080/api/workflow/greeting_workflow" \
  -H "Content-Type: application/json" \
  -d '{"userName": "Alice"}'

# Parallel workflow
curl -X POST "http://localhost:8080/api/workflow/parallel_fetch_workflow" \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-123"}'

# Conditional workflow (try different shipping types: standard, express, premium)
curl -X POST "http://localhost:8080/api/workflow/order_processing_workflow" \
  -H "Content-Type: application/json" \
  -d '{"shippingType": "express", "orderId": "ORD-456"}'
```

## Service Endpoints

Ensure all services are running:

```bash
docker compose -f docker/docker-compose.yml ps
```

Expected services:
| Service | Port | URL | Purpose |
|---------|------|-----|---------|
| Conductor UI | 5001 | http://localhost:5001 | Start/monitor workflows |
| Temporal | 7234 | gRPC | Temporal server |
| Temporal UI | 8234 | http://localhost:8234 | View Temporal workflows |
| Conductor Server | 8080 | http://localhost:8080 | Conductor REST API |
| Swagger UI | 8080 | http://localhost:8080/swagger-ui.html | API documentation |

## Demo 1: Simple Sequential Workflow

### Step 1: Register Task Definitions

```bash
curl -X POST http://localhost:8080/api/metadata/taskdefs \
  -H "Content-Type: application/json" \
  -d '[
    {"name": "greet", "timeoutSeconds": 0, "responseTimeoutSeconds": 0},
    {"name": "process", "timeoutSeconds": 0, "responseTimeoutSeconds": 0},
    {"name": "notify", "timeoutSeconds": 0, "responseTimeoutSeconds": 0}
  ]'
```

### Step 2: Register Workflow Definition

```bash
curl -X POST http://localhost:8080/api/metadata/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "greeting_workflow",
    "version": 1,
    "tasks": [
      {
        "name": "greet",
        "taskReferenceName": "greet_task",
        "type": "SIMPLE",
        "inputParameters": {
          "name": "${workflow.input.userName}"
        }
      },
      {
        "name": "process",
        "taskReferenceName": "process_task",
        "type": "SIMPLE",
        "inputParameters": {
          "greeting": "${greet_task.output.message}"
        }
      },
      {
        "name": "notify",
        "taskReferenceName": "notify_task",
        "type": "SIMPLE",
        "inputParameters": {
          "result": "${process_task.output.result}"
        }
      }
    ],
    "outputParameters": {
      "finalMessage": "${notify_task.output.notification}"
    }
  }'
```

### Step 3: Start Workflow Execution

```bash
curl -X POST http://localhost:8080/api/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "greeting_workflow",
    "version": 1,
    "input": {
      "userName": "Alice"
    }
  }'
```

This returns the workflow ID. Save it for later:
```bash
WORKFLOW_ID=<returned-id>
```

### Step 4: Check Workflow Status

```bash
curl http://localhost:8080/api/workflow/$WORKFLOW_ID | jq
```

### Step 5: View in Temporal UI

- **Temporal UI**: http://localhost:8233 - See workflow execution, event history, and search attributes

---

## Demo 2: Parallel Execution (FORK_JOIN)

### Register Workflow with Parallel Tasks

```bash
curl -X POST http://localhost:8080/api/metadata/taskdefs \
  -H "Content-Type: application/json" \
  -d '[
    {"name": "fetch_user", "timeoutSeconds": 0},
    {"name": "fetch_orders", "timeoutSeconds": 0},
    {"name": "fetch_recommendations", "timeoutSeconds": 0},
    {"name": "aggregate", "timeoutSeconds": 0}
  ]'

curl -X POST http://localhost:8080/api/metadata/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "parallel_fetch_workflow",
    "version": 1,
    "tasks": [
      {
        "name": "fork_fetch",
        "taskReferenceName": "fork_task",
        "type": "FORK_JOIN",
        "forkTasks": [
          [
            {
              "name": "fetch_user",
              "taskReferenceName": "user_task",
              "type": "SIMPLE",
              "inputParameters": {"userId": "${workflow.input.userId}"}
            }
          ],
          [
            {
              "name": "fetch_orders",
              "taskReferenceName": "orders_task",
              "type": "SIMPLE",
              "inputParameters": {"userId": "${workflow.input.userId}"}
            }
          ],
          [
            {
              "name": "fetch_recommendations",
              "taskReferenceName": "recs_task",
              "type": "SIMPLE",
              "inputParameters": {"userId": "${workflow.input.userId}"}
            }
          ]
        ]
      },
      {
        "name": "join_fetch",
        "taskReferenceName": "join_task",
        "type": "JOIN",
        "joinOn": ["user_task", "orders_task", "recs_task"]
      },
      {
        "name": "aggregate",
        "taskReferenceName": "aggregate_task",
        "type": "SIMPLE",
        "inputParameters": {
          "user": "${user_task.output}",
          "orders": "${orders_task.output}",
          "recommendations": "${recs_task.output}"
        }
      }
    ]
  }'
```

### Start Parallel Workflow

```bash
curl -X POST http://localhost:8080/api/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "parallel_fetch_workflow",
    "version": 1,
    "input": {"userId": "user-123"}
  }'
```

---

## Demo 3: Conditional Logic (SWITCH)

### Register Conditional Workflow

```bash
curl -X POST http://localhost:8080/api/metadata/taskdefs \
  -H "Content-Type: application/json" \
  -d '[
    {"name": "process_standard", "timeoutSeconds": 0},
    {"name": "process_express", "timeoutSeconds": 0},
    {"name": "process_premium", "timeoutSeconds": 0},
    {"name": "finalize", "timeoutSeconds": 0}
  ]'

curl -X POST http://localhost:8080/api/metadata/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "order_processing_workflow",
    "version": 1,
    "tasks": [
      {
        "name": "route_order",
        "taskReferenceName": "switch_task",
        "type": "SWITCH",
        "evaluatorType": "value-param",
        "expression": "shippingType",
        "inputParameters": {
          "shippingType": "${workflow.input.shippingType}"
        },
        "decisionCases": {
          "standard": [
            {
              "name": "process_standard",
              "taskReferenceName": "standard_task",
              "type": "SIMPLE"
            }
          ],
          "express": [
            {
              "name": "process_express",
              "taskReferenceName": "express_task",
              "type": "SIMPLE"
            }
          ],
          "premium": [
            {
              "name": "process_premium",
              "taskReferenceName": "premium_task",
              "type": "SIMPLE"
            }
          ]
        },
        "defaultCase": [
          {
            "name": "process_standard",
            "taskReferenceName": "default_task",
            "type": "SIMPLE"
          }
        ]
      },
      {
        "name": "finalize",
        "taskReferenceName": "finalize_task",
        "type": "SIMPLE"
      }
    ]
  }'
```

### Start Conditional Workflow

```bash
# Express shipping
curl -X POST http://localhost:8080/api/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "order_processing_workflow",
    "version": 1,
    "input": {"shippingType": "express", "orderId": "ORD-456"}
  }'

# Premium shipping
curl -X POST http://localhost:8080/api/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "order_processing_workflow",
    "version": 1,
    "input": {"shippingType": "premium", "orderId": "ORD-789"}
  }'
```

---

## Demo 4: Loop (DO_WHILE)

### Register Loop Workflow

```bash
curl -X POST http://localhost:8080/api/metadata/taskdefs \
  -H "Content-Type: application/json" \
  -d '[
    {"name": "process_batch", "timeoutSeconds": 0},
    {"name": "check_more", "timeoutSeconds": 0}
  ]'

curl -X POST http://localhost:8080/api/metadata/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "batch_processing_workflow",
    "version": 1,
    "tasks": [
      {
        "name": "batch_loop",
        "taskReferenceName": "loop_task",
        "type": "DO_WHILE",
        "loopCondition": "if ($.loop_task['iteration'] < $.batchCount) { true; } else { false; }",
        "loopOver": [
          {
            "name": "process_batch",
            "taskReferenceName": "batch_task",
            "type": "SIMPLE",
            "inputParameters": {
              "batchNumber": "${loop_task.output.iteration}"
            }
          }
        ],
        "inputParameters": {
          "batchCount": "${workflow.input.totalBatches}"
        }
      }
    ]
  }'
```

### Start Loop Workflow

```bash
curl -X POST http://localhost:8080/api/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "batch_processing_workflow",
    "version": 1,
    "input": {"totalBatches": 5}
  }'
```

---

## Demo 5: Human Task (Manual Approval)

HUMAN tasks wait indefinitely for manual approval or completion via signal or REST API.

### Register Workflow with HUMAN Task

```bash
curl -X POST http://localhost:8080/api/metadata/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "approval_workflow",
    "version": 1,
    "tasks": [
      {
        "name": "approval_task",
        "taskReferenceName": "approval_ref",
        "type": "HUMAN",
        "inputParameters": {
          "requestId": "${workflow.input.requestId}",
          "amount": "${workflow.input.amount}"
        }
      }
    ]
  }'
```

### Start Approval Workflow

```bash
curl -X POST http://localhost:8080/api/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "approval_workflow",
    "version": 1,
    "input": {"requestId": "REQ-12345", "amount": 5000}
  }'
```

Save the workflow ID:
```bash
WORKFLOW_ID=<returned-id>
```

### Check HUMAN Task Status

```bash
# Get workflow status - HUMAN task will be IN_PROGRESS
curl http://localhost:8080/api/workflow/$WORKFLOW_ID | jq '.tasks[] | select(.taskReferenceName=="approval_ref")'
```

### Complete HUMAN Task via REST API

```bash
# Get the taskId from the workflow status first
TASK_ID=$(curl -s http://localhost:8080/api/workflow/$WORKFLOW_ID | jq -r '.tasks[] | select(.taskReferenceName=="approval_ref") | .taskId')

# Complete the task
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d "{
    \"taskId\": \"$TASK_ID\",
    \"workflowInstanceId\": \"$WORKFLOW_ID\",
    \"status\": \"COMPLETED\",
    \"outputData\": {\"approved\": true, \"approver\": \"manager@company.com\"}
  }"
```

### Verify Completion

```bash
curl http://localhost:8080/api/workflow/$WORKFLOW_ID | jq '{status, output}'
```

---

## Demo 6: Sub-Workflow (Nested Workflows)

SUB_WORKFLOW tasks invoke other workflows, enabling modular workflow composition.

### Register Child Workflow

```bash
# Task for child workflow
curl -X POST http://localhost:8080/api/metadata/taskdefs \
  -H "Content-Type: application/json" \
  -d '[{"name": "child_task", "timeoutSeconds": 0}]'

# Child workflow definition
curl -X POST http://localhost:8080/api/metadata/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "child_workflow",
    "version": 1,
    "tasks": [
      {
        "name": "child_task",
        "taskReferenceName": "child_task_ref",
        "type": "SIMPLE"
      }
    ],
    "outputParameters": {
      "childResult": "${child_task_ref.output}"
    }
  }'
```

### Register Parent Workflow with SUB_WORKFLOW

```bash
curl -X POST http://localhost:8080/api/metadata/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "parent_workflow",
    "version": 1,
    "tasks": [
      {
        "name": "sub_workflow_task",
        "taskReferenceName": "sub_workflow_ref",
        "type": "SUB_WORKFLOW",
        "subWorkflowParam": {
          "name": "child_workflow",
          "version": 1
        },
        "inputParameters": {
          "workflowInput": "${workflow.input}"
        }
      }
    ],
    "outputParameters": {
      "parentResult": "${sub_workflow_ref.output}"
    }
  }'
```

### Start Parent Workflow

```bash
curl -X POST http://localhost:8080/api/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "parent_workflow",
    "version": 1,
    "input": {"message": "Hello from parent"}
  }'
```

### View in Temporal UI

In the Temporal UI, you'll see two workflows:
1. The parent workflow (parent_workflow)
2. The child workflow (child_workflow) - started by the parent

---

## Demo 7: JSON_JQ_TRANSFORM (Data Transformation)

Transform data without writing code using JQ expressions.

### Register JQ Transform Workflow

```bash
curl -X POST http://localhost:8080/api/metadata/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "jq_transform_workflow",
    "version": 1,
    "tasks": [
      {
        "name": "transform_data",
        "taskReferenceName": "jq_transform_ref",
        "type": "JSON_JQ_TRANSFORM",
        "inputParameters": {
          "queryExpression": "{ fullName: (.data.firstName + \" \" + .data.lastName), location: .data.city, isAdult: (.data.age >= 18) }",
          "data": "${workflow.input}"
        }
      }
    ],
    "outputParameters": {
      "transformed": "${jq_transform_ref.output.result}"
    }
  }'
```

### Start JQ Transform Workflow

```bash
curl -X POST http://localhost:8080/api/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "jq_transform_workflow",
    "version": 1,
    "input": {
      "firstName": "John",
      "lastName": "Doe",
      "age": 30,
      "city": "New York"
    }
  }'
```

### Example: Sum Array Values

```bash
# Register workflow that sums array values
curl -X POST http://localhost:8080/api/metadata/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "sum_values_workflow",
    "version": 1,
    "tasks": [
      {
        "name": "sum_task",
        "taskReferenceName": "sum_ref",
        "type": "JSON_JQ_TRANSFORM",
        "inputParameters": {
          "queryExpression": "[.data.items[].value] | add",
          "data": "${workflow.input}"
        }
      }
    ]
  }'

# Start with array input
curl -X POST http://localhost:8080/api/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "sum_values_workflow",
    "version": 1,
    "input": {
      "items": [
        {"id": 1, "value": 10},
        {"id": 2, "value": 20},
        {"id": 3, "value": 30}
      ]
    }
  }'
```

---

## Demo 8: EVENT Task (Publish Events)

EVENT tasks publish messages to external systems (SQS, Kafka, etc.).

### Register EVENT Workflow

```bash
curl -X POST http://localhost:8080/api/metadata/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "event_workflow",
    "version": 1,
    "tasks": [
      {
        "name": "publish_event",
        "taskReferenceName": "event_ref",
        "type": "EVENT",
        "sink": "conductor:order-events",
        "inputParameters": {
          "orderId": "${workflow.input.orderId}",
          "customerId": "${workflow.input.customerId}",
          "eventType": "ORDER_CREATED"
        }
      }
    ]
  }'
```

### Start EVENT Workflow

```bash
curl -X POST http://localhost:8080/api/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "event_workflow",
    "version": 1,
    "input": {"orderId": "ORD-12345", "customerId": "CUST-789"}
  }'
```

### Supported Sink Formats

| Sink Format | Example | Description |
|-------------|---------|-------------|
| `conductor:` | `conductor:my-event` | Internal Conductor event |
| `sqs:` | `sqs:my-sqs-queue` | Amazon SQS |
| `kafka:` | `kafka:order-topic` | Apache Kafka |

---

## Demo 9: Pause and Resume Workflow

Control workflow execution by pausing and resuming.

### Start a Slow Workflow

```bash
# Register slow task
curl -X POST http://localhost:8080/api/metadata/taskdefs \
  -H "Content-Type: application/json" \
  -d '[{"name": "slow_task", "timeoutSeconds": 0}]'

# Register workflow with slow task
curl -X POST http://localhost:8080/api/metadata/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "pausable_workflow",
    "version": 1,
    "tasks": [
      {
        "name": "slow_task",
        "taskReferenceName": "slow_task_ref",
        "type": "SIMPLE",
        "inputParameters": {"delayMs": 30000}
      }
    ]
  }'

# Start workflow
WORKFLOW_ID=$(curl -s -X POST http://localhost:8080/api/workflow \
  -H "Content-Type: application/json" \
  -d '{"name": "pausable_workflow", "version": 1, "input": {}}' | tr -d '"')

echo "Workflow ID: $WORKFLOW_ID"
```

### Pause the Workflow

```bash
# Pause
curl -X PUT "http://localhost:8080/api/workflow/$WORKFLOW_ID/pause"

# Check status - should be PAUSED
curl http://localhost:8080/api/workflow/$WORKFLOW_ID/status | jq '.status'
```

### Resume the Workflow

```bash
# Resume
curl -X PUT "http://localhost:8080/api/workflow/$WORKFLOW_ID/resume"

# Check status - should be RUNNING
curl http://localhost:8080/api/workflow/$WORKFLOW_ID/status | jq '.status'
```

### View in Temporal UI

When paused, the workflow's `ConductorStatus` search attribute shows `PAUSED`.

---

## Advanced Search Queries

The search API supports SQL-like query syntax with AND conditions, IN clauses, and more.

### Search by Status

```bash
# Using equals syntax
curl "http://localhost:8080/api/workflow/search?query=status=COMPLETED"

# Using legacy colon syntax
curl "http://localhost:8080/api/workflow/search?query=status:RUNNING"
```

### Search by Workflow Type

```bash
curl "http://localhost:8080/api/workflow/search?query=workflowType=greeting_workflow"
```

### Search with AND Conditions

```bash
# Find completed workflows of a specific type
curl "http://localhost:8080/api/workflow/search?query=status=COMPLETED%20AND%20workflowType=greeting_workflow"
```

### Search with IN Clause

```bash
# Find workflows with multiple statuses
curl "http://localhost:8080/api/workflow/search?query=status%20IN%20(RUNNING,PAUSED)"
```

### Search by Correlation ID

```bash
curl "http://localhost:8080/api/workflow/search?query=correlationId=order-12345"
```

### Supported Query Fields

| Field | Description | Example |
|-------|-------------|---------|
| `status` | Workflow status | `status=COMPLETED` |
| `workflowType` | Workflow definition name | `workflowType=my_workflow` |
| `correlationId` | Correlation identifier | `correlationId=order-123` |
| `startTime` | Start timestamp (ms) | `startTime>1704067200000` |
| `updateTime` | Last update timestamp | `updateTime>1704067200000` |

### Supported Operators

| Operator | Example |
|----------|---------|
| `=` | `status=RUNNING` |
| `!=` | `status!=FAILED` |
| `>`, `<`, `>=`, `<=` | `startTime>1704067200000` |
| `IN` | `status IN (RUNNING, PAUSED)` |
| `AND` | `status=COMPLETED AND workflowType=my_wf` |

---

## API Quick Reference

### Metadata Operations

```bash
# List all workflow definitions
curl http://localhost:8080/api/metadata/workflow | jq

# Get specific workflow definition
curl http://localhost:8080/api/metadata/workflow/greeting_workflow | jq

# List all task definitions
curl http://localhost:8080/api/metadata/taskdefs | jq

# Delete workflow definition
curl -X DELETE http://localhost:8080/api/metadata/workflow/greeting_workflow/1
```

### Workflow Operations

```bash
# Start workflow
curl -X POST http://localhost:8080/api/workflow \
  -H "Content-Type: application/json" \
  -d '{"name": "workflow_name", "version": 1, "input": {}}'

# Get workflow by ID
curl http://localhost:8080/api/workflow/{workflowId} | jq

# Get workflow status
curl http://localhost:8080/api/workflow/{workflowId}/status | jq

# Search workflows
curl "http://localhost:8080/api/workflow/search?query=status:RUNNING" | jq

# Terminate workflow
curl -X DELETE "http://localhost:8080/api/workflow/{workflowId}?reason=demo"

# Pause workflow
curl -X PUT http://localhost:8080/api/workflow/{workflowId}/pause

# Resume workflow
curl -X PUT http://localhost:8080/api/workflow/{workflowId}/resume
```

---

## What to Show During Demo

### 1. Architecture Overview
- Conductor workflow definitions are executed on Temporal
- No code generation - workflow is interpreted at runtime
- Temporal provides durability, retries, and observability

### 2. Conductor UI - Start Workflows (http://localhost:5001)
- Browse workflow definitions
- Start a workflow execution
- Monitor running workflows
- View workflow execution details

### 3. Swagger UI - Direct API Access (http://localhost:8080/swagger-ui.html)
- Show workflow and task APIs
- Register a workflow definition
- Start a workflow execution
- Query workflow status

### 4. Temporal UI - Execution Details (http://localhost:8233)
- See the workflow appear in real-time
- Click on the workflow to show:
  - **Event History**: Every state transition logged
  - **Pending Activities**: Tasks waiting for execution
  - **Search Attributes**: ConductorWorkflowType, ConductorStatus
- Show **Stack Trace** tab during activity execution
- Show **Queries** tab to query workflow state

### 5. Key Features to Highlight

| Feature | Demo | Key Point |
|---------|------|-----------|
| **Sequential Tasks** | Demo 1 | Tasks execute in order |
| **Parallel Execution** | Demo 2 | FORK_JOIN runs tasks concurrently |
| **Conditional Logic** | Demo 3 | SWITCH routes based on input |
| **Loops** | Demo 4 | DO_WHILE iterates until condition |
| **Human Approval** | Demo 5 | HUMAN tasks wait for manual completion |
| **Modular Workflows** | Demo 6 | SUB_WORKFLOW enables composition |
| **Data Transformation** | Demo 7 | JSON_JQ_TRANSFORM without code |
| **Event Publishing** | Demo 8 | EVENT sends to SQS/Kafka/etc |
| **Flow Control** | Demo 9 | Pause/resume running workflows |
| **Search** | Advanced Search | SQL-like queries with AND, IN |

### 6. Key Benefits
- **Durability**: Temporal's event sourcing survives crashes
- **Scalability**: Temporal scales to millions of workflows
- **Observability**: Full execution history in Temporal UI
- **Compatibility**: Run existing Conductor workflows unchanged
- **Human-in-the-Loop**: HUMAN tasks for approvals and manual steps
- **Extensibility**: JSON_JQ_TRANSFORM for codeless data transformation

---

## Troubleshooting

### Local Development

```bash
# Check Conductor server logs
tail -f /tmp/conductor-server.log

# Check Temporal server logs
tail -f /tmp/temporal-server.log

# Restart Temporal (clean state)
pkill -f "temporal server"
./scripts/start-temporal.sh

# Stop Conductor UI container
docker stop conductor-ui && docker rm conductor-ui
```

### Docker Compose

```bash
# Check service logs
docker compose -f docker/docker-compose.yml logs conductor-server -f
docker compose -f docker/docker-compose.yml logs temporal -f

# Restart services
docker compose -f docker/docker-compose.yml restart conductor-server

# Full reset
docker compose -f docker/docker-compose.yml down -v
docker compose -f docker/docker-compose.yml up -d
```

## Cleanup (Local Development)

```bash
# Stop all services
pkill -f "temporal server"
pkill -f "bootRun"
docker stop conductor-ui && docker rm conductor-ui
```
