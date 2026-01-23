#!/bin/bash
# Initialize Temporal search attributes and register demo workflows

set -e

TEMPORAL_ADDRESS=${TEMPORAL_ADDRESS:-temporal:7233}
CONDUCTOR_ADDRESS=${CONDUCTOR_ADDRESS:-conductor-server:8080}
TEMPORAL_NAMESPACE=${TEMPORAL_NAMESPACE:-default}

echo "Waiting for Temporal to be ready..."
until temporal operator cluster health --address $TEMPORAL_ADDRESS 2>/dev/null | grep -q "SERVING"; do
    echo "  Temporal not ready, waiting..."
    sleep 5
done
echo "Temporal is ready!"

echo ""
echo "Creating search attributes in namespace '$TEMPORAL_NAMESPACE'..."

# Create Keyword search attributes
for attr in ConductorWorkflowType ConductorStatus ConductorOwnerApp ConductorCorrelationId ConductorDefinitionType ConductorDefinitionName; do
    echo "  Creating $attr (Keyword)..."
    temporal operator search-attribute create \
        --address $TEMPORAL_ADDRESS \
        --namespace $TEMPORAL_NAMESPACE \
        --name $attr \
        --type Keyword 2>&1 || echo "    $attr may already exist"
done

# Create Int search attributes (Long in Java SDK)
# Note: ConductorDefinitionVersion removed due to Int attribute limit in dev PostgreSQL
for attr in ConductorPriority ConductorWorkflowVersion; do
    echo "  Creating $attr (Int)..."
    temporal operator search-attribute create \
        --address $TEMPORAL_ADDRESS \
        --namespace $TEMPORAL_NAMESPACE \
        --name $attr \
        --type Int 2>&1 || echo "    $attr may already exist"
done

# Create KeywordList search attributes
for attr in ConductorFailedTaskNames; do
    echo "  Creating $attr (KeywordList)..."
    temporal operator search-attribute create \
        --address $TEMPORAL_ADDRESS \
        --namespace $TEMPORAL_NAMESPACE \
        --name $attr \
        --type KeywordList 2>&1 || echo "    $attr may already exist"
done

# Verify search attributes were created
echo ""
echo "Verifying search attributes..."
temporal operator search-attribute list --address $TEMPORAL_ADDRESS --namespace $TEMPORAL_NAMESPACE 2>&1 | grep -E "Conductor" || true
echo "Search attributes ready!"

echo ""
echo "Waiting for Conductor Server to be ready..."
until curl -sf http://$CONDUCTOR_ADDRESS/actuator/health/liveness >/dev/null 2>&1; do
    echo "  Conductor not ready, waiting..."
    sleep 5
done
echo "Conductor is ready!"

# Give the server a moment to fully initialize
echo "Waiting for server to stabilize..."
sleep 5

echo ""
echo "Registering task definitions..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://$CONDUCTOR_ADDRESS/api/metadata/taskdefs \
  -H "Content-Type: application/json" \
  -d '[
    {"name": "greet", "timeoutSeconds": 0, "responseTimeoutSeconds": 0},
    {"name": "process", "timeoutSeconds": 0, "responseTimeoutSeconds": 0},
    {"name": "notify", "timeoutSeconds": 0, "responseTimeoutSeconds": 0},
    {"name": "fetch_user", "timeoutSeconds": 0, "responseTimeoutSeconds": 0},
    {"name": "fetch_orders", "timeoutSeconds": 0, "responseTimeoutSeconds": 0},
    {"name": "fetch_recommendations", "timeoutSeconds": 0, "responseTimeoutSeconds": 0},
    {"name": "aggregate", "timeoutSeconds": 0, "responseTimeoutSeconds": 0},
    {"name": "process_standard", "timeoutSeconds": 0, "responseTimeoutSeconds": 0},
    {"name": "process_express", "timeoutSeconds": 0, "responseTimeoutSeconds": 0},
    {"name": "process_premium", "timeoutSeconds": 0, "responseTimeoutSeconds": 0},
    {"name": "finalize", "timeoutSeconds": 0, "responseTimeoutSeconds": 0}
  ]')
if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "204" ]; then
    echo "  Task definitions registered"
else
    echo "  Task definitions registration returned HTTP $HTTP_CODE"
fi

echo ""
echo "Registering workflow definitions..."

# Function to register a workflow with retries
register_workflow() {
    local name=$1
    local json=$2
    local max_retries=3
    local retry=0

    while [ $retry -lt $max_retries ]; do
        HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://$CONDUCTOR_ADDRESS/api/metadata/workflow \
            -H "Content-Type: application/json" \
            -d "$json")

        if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "204" ]; then
            echo "  $name registered"
            return 0
        elif [ "$HTTP_CODE" = "409" ]; then
            echo "  $name already exists"
            return 0
        else
            retry=$((retry + 1))
            if [ $retry -lt $max_retries ]; then
                echo "  $name registration failed (HTTP $HTTP_CODE), retrying in 2s..."
                sleep 2
            else
                echo "  $name registration failed (HTTP $HTTP_CODE) after $max_retries attempts"
                return 1
            fi
        fi
    done
}

# Simple sequential workflow
register_workflow "greeting_workflow" '{
    "name": "greeting_workflow",
    "version": 1,
    "description": "Simple sequential workflow that greets a user",
    "tasks": [
      {
        "name": "greet",
        "taskReferenceName": "greet_task",
        "type": "SIMPLE",
        "inputParameters": {"name": "${workflow.input.userName}"}
      },
      {
        "name": "process",
        "taskReferenceName": "process_task",
        "type": "SIMPLE",
        "inputParameters": {"greeting": "${greet_task.output.message}"}
      },
      {
        "name": "notify",
        "taskReferenceName": "notify_task",
        "type": "SIMPLE",
        "inputParameters": {"result": "${process_task.output.result}"}
      }
    ],
    "outputParameters": {"finalMessage": "${notify_task.output.notification}"}
  }'

# Parallel workflow (FORK_JOIN)
register_workflow "parallel_fetch_workflow" '{
    "name": "parallel_fetch_workflow",
    "version": 1,
    "description": "Parallel data fetching workflow",
    "tasks": [
      {
        "name": "fork_fetch",
        "taskReferenceName": "fork_task",
        "type": "FORK_JOIN",
        "forkTasks": [
          [{"name": "fetch_user", "taskReferenceName": "user_task", "type": "SIMPLE", "inputParameters": {"userId": "${workflow.input.userId}"}}],
          [{"name": "fetch_orders", "taskReferenceName": "orders_task", "type": "SIMPLE", "inputParameters": {"userId": "${workflow.input.userId}"}}],
          [{"name": "fetch_recommendations", "taskReferenceName": "recs_task", "type": "SIMPLE", "inputParameters": {"userId": "${workflow.input.userId}"}}]
        ]
      },
      {"name": "join_fetch", "taskReferenceName": "join_task", "type": "JOIN", "joinOn": ["user_task", "orders_task", "recs_task"]},
      {"name": "aggregate", "taskReferenceName": "aggregate_task", "type": "SIMPLE", "inputParameters": {"user": "${user_task.output}", "orders": "${orders_task.output}", "recommendations": "${recs_task.output}"}}
    ]
  }'

# Conditional workflow (SWITCH)
register_workflow "order_processing_workflow" '{
    "name": "order_processing_workflow",
    "version": 1,
    "description": "Conditional order processing based on shipping type",
    "tasks": [
      {
        "name": "route_order",
        "taskReferenceName": "switch_task",
        "type": "SWITCH",
        "evaluatorType": "value-param",
        "expression": "shippingType",
        "inputParameters": {"shippingType": "${workflow.input.shippingType}"},
        "decisionCases": {
          "standard": [{"name": "process_standard", "taskReferenceName": "standard_task", "type": "SIMPLE"}],
          "express": [{"name": "process_express", "taskReferenceName": "express_task", "type": "SIMPLE"}],
          "premium": [{"name": "process_premium", "taskReferenceName": "premium_task", "type": "SIMPLE"}]
        },
        "defaultCase": [{"name": "process_standard", "taskReferenceName": "default_task", "type": "SIMPLE"}]
      },
      {"name": "finalize", "taskReferenceName": "finalize_task", "type": "SIMPLE"}
    ]
  }'

echo ""
echo "========================================="
echo "Demo initialization complete!"
echo ""
echo "Available workflows:"
echo "  - greeting_workflow (sequential)"
echo "  - parallel_fetch_workflow (fork/join)"
echo "  - order_processing_workflow (conditional)"
echo ""
echo "Try running:"
echo "  curl -X POST http://localhost:8081/api/workflow/greeting_workflow -H 'Content-Type: application/json' -d '{\"userName\": \"Alice\"}'"
echo "========================================="
