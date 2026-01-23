# Temporal Conductor Demo

This guide demonstrates how to use Conductor workflows executed on Temporal.

## Quick Start

```bash
# Start all services (includes automatic demo initialization)
docker compose up -d

# Wait for initialization to complete (check logs)
docker logs demo-init -f

# Services will be ready at:
# - Conductor UI:     http://localhost:5001
# - Conductor API:    http://localhost:8081
# - Conductor Swagger: http://localhost:8081/swagger-ui.html
# - Temporal UI:      http://localhost:8088
```

The `demo-init` container automatically:
1. Creates Temporal search attributes (ConductorWorkflowType, ConductorStatus, etc.)
2. Registers sample task definitions
3. Registers 3 demo workflows:
   - `greeting_workflow` - Sequential workflow
   - `parallel_fetch_workflow` - Fork/Join parallel execution
   - `order_processing_workflow` - Conditional SWITCH logic

## Running Demo Workflows

```bash
# Simple sequential workflow
curl -X POST "http://localhost:8081/api/workflow/greeting_workflow" \
  -H "Content-Type: application/json" \
  -d '{"userName": "Alice"}'

# Parallel workflow
curl -X POST "http://localhost:8081/api/workflow/parallel_fetch_workflow" \
  -H "Content-Type: application/json" \
  -d '{"userId": "user-123"}'

# Conditional workflow (try different shipping types: standard, express, premium)
curl -X POST "http://localhost:8081/api/workflow/order_processing_workflow" \
  -H "Content-Type: application/json" \
  -d '{"shippingType": "express", "orderId": "ORD-456"}'
```

## Service Endpoints

Ensure all services are running:

```bash
docker compose ps
```

Expected services:
| Service | Port | URL | Purpose |
|---------|------|-----|---------|
| Temporal | 7233 | gRPC | Temporal server |
| Temporal UI | 8088 | http://localhost:8088 | View Temporal workflows |
| Conductor Server | 8081 | http://localhost:8081 | Conductor REST API |
| Conductor Swagger | 8081 | http://localhost:8081/swagger-ui.html | API documentation |
| **Conductor UI** | 5001 | http://localhost:5001 | **Full UI with workflow execution** |

> **Note**: The Conductor UI uses the official `conductoross/conductor-standalone` image running in nginx-only mode,
> proxying API calls to our Temporal-backed Conductor server.

## Demo 1: Simple Sequential Workflow

### Step 1: Register Task Definitions

```bash
curl -X POST http://localhost:8081/api/metadata/taskdefs \
  -H "Content-Type: application/json" \
  -d '[
    {"name": "greet", "timeoutSeconds": 0, "responseTimeoutSeconds": 0},
    {"name": "process", "timeoutSeconds": 0, "responseTimeoutSeconds": 0},
    {"name": "notify", "timeoutSeconds": 0, "responseTimeoutSeconds": 0}
  ]'
```

### Step 2: Register Workflow Definition

```bash
curl -X POST http://localhost:8081/api/metadata/workflow \
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
curl -X POST http://localhost:8081/api/workflow \
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
curl http://localhost:8081/api/workflow/$WORKFLOW_ID | jq
```

### Step 5: View in UIs

- **Conductor UI**: http://localhost:5001 - See workflow definition and execution
- **Temporal UI**: http://localhost:8088 - See underlying Temporal workflow execution

---

## Demo 2: Parallel Execution (FORK_JOIN)

### Register Workflow with Parallel Tasks

```bash
curl -X POST http://localhost:8081/api/metadata/taskdefs \
  -H "Content-Type: application/json" \
  -d '[
    {"name": "fetch_user", "timeoutSeconds": 0},
    {"name": "fetch_orders", "timeoutSeconds": 0},
    {"name": "fetch_recommendations", "timeoutSeconds": 0},
    {"name": "aggregate", "timeoutSeconds": 0}
  ]'

curl -X POST http://localhost:8081/api/metadata/workflow \
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
curl -X POST http://localhost:8081/api/workflow \
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
curl -X POST http://localhost:8081/api/metadata/taskdefs \
  -H "Content-Type: application/json" \
  -d '[
    {"name": "process_standard", "timeoutSeconds": 0},
    {"name": "process_express", "timeoutSeconds": 0},
    {"name": "process_premium", "timeoutSeconds": 0},
    {"name": "finalize", "timeoutSeconds": 0}
  ]'

curl -X POST http://localhost:8081/api/metadata/workflow \
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
curl -X POST http://localhost:8081/api/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "order_processing_workflow",
    "version": 1,
    "input": {"shippingType": "express", "orderId": "ORD-456"}
  }'

# Premium shipping
curl -X POST http://localhost:8081/api/workflow \
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
curl -X POST http://localhost:8081/api/metadata/taskdefs \
  -H "Content-Type: application/json" \
  -d '[
    {"name": "process_batch", "timeoutSeconds": 0},
    {"name": "check_more", "timeoutSeconds": 0}
  ]'

curl -X POST http://localhost:8081/api/metadata/workflow \
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
curl -X POST http://localhost:8081/api/workflow \
  -H "Content-Type: application/json" \
  -d '{
    "name": "batch_processing_workflow",
    "version": 1,
    "input": {"totalBatches": 5}
  }'
```

---

## API Quick Reference

### Metadata Operations

```bash
# List all workflow definitions
curl http://localhost:8081/api/metadata/workflow | jq

# Get specific workflow definition
curl http://localhost:8081/api/metadata/workflow/greeting_workflow | jq

# List all task definitions
curl http://localhost:8081/api/metadata/taskdefs | jq

# Delete workflow definition
curl -X DELETE http://localhost:8081/api/metadata/workflow/greeting_workflow/1
```

### Workflow Operations

```bash
# Start workflow
curl -X POST http://localhost:8081/api/workflow \
  -H "Content-Type: application/json" \
  -d '{"name": "workflow_name", "version": 1, "input": {}}'

# Get workflow by ID
curl http://localhost:8081/api/workflow/{workflowId} | jq

# Get workflow status
curl http://localhost:8081/api/workflow/{workflowId}/status | jq

# Search workflows
curl "http://localhost:8081/api/workflow/search?query=status:RUNNING" | jq

# Terminate workflow
curl -X DELETE "http://localhost:8081/api/workflow/{workflowId}?reason=demo"

# Pause workflow
curl -X PUT http://localhost:8081/api/workflow/{workflowId}/pause

# Resume workflow
curl -X PUT http://localhost:8081/api/workflow/{workflowId}/resume
```

---

## What to Show During Demo

### 1. Architecture Overview
- Conductor workflow definitions are executed on Temporal
- No code generation - workflow is interpreted at runtime
- Temporal provides durability, retries, and observability

### 2. Conductor UI - Execute Workflows (http://localhost:5001)
- Navigate to **Workbench** to execute workflows
- Or go to **Definitions** → select a workflow → click **Run Workflow**
- Enter input parameters and execute
- View execution status and task progress

### 3. Conductor UI - View Definitions (http://localhost:5001)
- Show workflow definitions (under Definitions)
- Show workflow executions (under Executions)
- Show task status in execution detail view

### 4. Temporal UI (http://localhost:8088)
- Show the underlying Temporal workflow
- Show event history
- Show search attributes (ConductorWorkflowType, ConductorStatus)

### 5. Key Benefits
- **Durability**: Temporal's event sourcing survives crashes
- **Scalability**: Temporal scales to millions of workflows
- **Observability**: Full execution history in Temporal UI
- **Compatibility**: Run existing Conductor workflows unchanged

---

## Troubleshooting

### Check service logs
```bash
docker compose logs conductor-server -f
docker compose logs temporal -f
```

### Restart services
```bash
docker compose restart conductor-server
```

### Full reset
```bash
docker compose down -v
docker compose up -d
```
