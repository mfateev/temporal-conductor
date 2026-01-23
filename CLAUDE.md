# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Temporal Conductor** executes [Netflix Conductor](https://conductor-oss.github.io/conductor/) workflow definitions using Temporal as the execution backend. A single Temporal workflow interprets a Conductor workflow definition at runtime (no code generation).

**Core pattern**: Reuse Conductor's libraries (DeciderService, expression evaluation, task mappers) with in-memory DAO implementations. Temporal provides durability via event history, eliminating need for external databases.

## Build & Test Commands

```bash
# Build and test the server (main implementation)
cd conductor-server-temporal
./gradlew build                    # Full build with tests
./gradlew test                     # Run all 90 tests
./gradlew test --tests "*WorkflowTest*"   # Pattern matching
./gradlew bootRun                  # Run server (requires Temporal)

# Build the core library
cd conductor-temporal-ext
./gradlew build
./gradlew publishToMavenLocal      # Publish for local use
```

**Requirements**: JDK 21, Gradle wrapper included

## Project Structure

```
temporal-conductor/
├── conductor-server-temporal/    # Spring Boot REST server (PRODUCTION)
├── conductor-temporal-ext/       # Core library (workflows, executors, DAOs)
├── conductor/                    # Netflix Conductor OSS (git worktree, reference)
├── design/                       # Design documents
└── archive/                      # Archived POC modules (reference only)
    ├── poc-1-decider-isolation/
    ├── poc-2-determinism-audit/
    └── poc-3-temporal-integration/
```

## Key Files

**Core Library** (conductor-temporal-ext):
- `src/main/java/io/temporal/conductor/workflow/ConductorWorkflowImpl.java` - Main Temporal workflow
- `src/main/java/io/temporal/conductor/executor/TemporalDeciderServiceFactory.java` - Factory for DeciderService
- `src/main/java/io/temporal/conductor/executor/SystemTaskExecutor.java` - FORK, JOIN, SWITCH, DO_WHILE
- `src/main/java/io/temporal/conductor/executor/InMemory*.java` - In-memory DAO implementations

**REST Server** (conductor-server-temporal):
- `src/main/java/io/temporal/conductor/api/*Resource.java` - REST endpoints
- `src/main/java/io/temporal/conductor/service/temporal/*Service.java` - Temporal backend
- `src/main/java/io/temporal/conductor/config/TemporalConfig.java` - Spring configuration

## Architecture

```
┌─────────────────────────────────────────────────┐
│  REST API Layer (Spring Boot)                   │
│  - WorkflowResource, TaskResource, etc.         │
└─────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────┐
│  Service Layer                                  │
│  - TemporalWorkflowService                      │
│  - TemporalTaskService                          │
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
│  - TaskExecutionActivities for SIMPLE/HTTP      │
└─────────────────────────────────────────────────┘
```

**Task type support**: SIMPLE, HTTP, FORK_JOIN, JOIN, SWITCH/DECISION, DO_WHILE, SET_VARIABLE, TERMINATE all working. SUB_WORKFLOW and WAIT not yet tested.

## Running Locally

```bash
# Start Temporal dev server
temporal server start-dev --namespace conductor

# Register search attributes (first time only)
temporal operator search-attribute create --namespace conductor \
  --name ConductorWorkflowType --type Keyword
temporal operator search-attribute create --namespace conductor \
  --name ConductorStatus --type Keyword

# Start the server with Temporal profile
cd conductor-server-temporal
SPRING_PROFILES_ACTIVE=temporal ./gradlew bootRun

# Or use stub profile for testing without Temporal
SPRING_PROFILES_ACTIVE=stub ./gradlew bootRun
```

**Swagger UI**: http://localhost:8080/swagger-ui.html

## Determinism Requirements

Temporal workflows must be deterministic for replay. Key mitigations:

1. **ID generation**: `TemporalIdGenerator` produces sequential IDs instead of `UUID.randomUUID()`
2. **Timestamps**: Use `Workflow.currentTimeMillis()` instead of `System.currentTimeMillis()`
3. **Timeouts**: Disable Conductor timeouts (set to 0), use Temporal timeouts instead

## Design Documents

See `design/` directory:
- `04-architecture.md` - Core architecture with diagrams
- `08-timeout-mapping.md` - Conductor to Temporal timeout mapping
- `11-phase1-workflow-observability.md` - Temporal integration spec
- `12-phase2-rest-api-layer.md` - REST API specification

## Git Workflow

**Push after every commit**: Always push changes to `origin` immediately after making a commit.

```bash
git push origin <branch>
```

## Checkpoints

**Do not rely on sprite checkpoints** for preserving work. Git commits pushed to remote are the primary mechanism for durability.
