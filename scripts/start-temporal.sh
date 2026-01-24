#!/bin/bash
#
# Start Temporal dev server and register Conductor search attributes
#
# Usage: ./scripts/start-temporal.sh
#

set -e

NAMESPACE="conductor"

echo "=== Temporal Conductor Setup ==="

# Check if Temporal server is already running
if pgrep -f "temporal server" > /dev/null 2>&1; then
    echo "✓ Temporal server is already running"
else
    echo "Starting Temporal dev server..."
    temporal server start-dev --namespace "$NAMESPACE" > /tmp/temporal-server.log 2>&1 &

    # Wait for server to be ready
    echo -n "Waiting for server to start"
    for i in {1..30}; do
        if temporal operator namespace describe "$NAMESPACE" > /dev/null 2>&1; then
            echo ""
            echo "✓ Temporal server started"
            break
        fi
        echo -n "."
        sleep 1
    done

    if ! pgrep -f "temporal server" > /dev/null 2>&1; then
        echo ""
        echo "✗ Failed to start Temporal server"
        echo "Check logs: tail -f /tmp/temporal-server.log"
        exit 1
    fi
fi

# Ensure namespace exists
if ! temporal operator namespace describe "$NAMESPACE" > /dev/null 2>&1; then
    echo "Creating namespace: $NAMESPACE"
    temporal operator namespace create "$NAMESPACE"
    sleep 2
fi

echo "✓ Namespace '$NAMESPACE' is ready"

# Register search attributes (idempotent - will skip if already exists)
echo ""
echo "Registering search attributes..."

register_attribute() {
    local name=$1
    local type=$2

    # Create attribute (idempotent - Temporal will ignore if already exists)
    output=$(temporal operator search-attribute create -n "$NAMESPACE" --name "$name" --type "$type" 2>&1)
    if echo "$output" | grep -q "already exists"; then
        echo "  ✓ $name (already exists)"
    elif echo "$output" | grep -q "have been added"; then
        echo "  ✓ $name ($type)"
    else
        echo "  ✗ $name - $output"
    fi
}

echo "Definition storage attributes:"
register_attribute "ConductorDefinitionType" "Keyword"
register_attribute "ConductorDefinitionName" "Keyword"
register_attribute "ConductorDefinitionVersion" "Int"

echo "Workflow execution attributes:"
register_attribute "ConductorWorkflowType" "Keyword"
register_attribute "ConductorWorkflowVersion" "Int"
register_attribute "ConductorStatus" "Keyword"
register_attribute "ConductorCorrelationId" "Keyword"
register_attribute "ConductorPriority" "Int"
register_attribute "ConductorFailedTaskNames" "KeywordList"
register_attribute "ConductorOwnerApp" "Keyword"

echo ""
echo "=== Setup Complete ==="
echo ""
echo "Temporal UI: http://localhost:8233"
echo ""
echo "To start the Conductor server:"
echo "  SPRING_PROFILES_ACTIVE=temporal ./gradlew :temporal-conductor:bootRun"
echo ""
echo "Swagger UI will be at: http://localhost:8080/swagger-ui.html"
