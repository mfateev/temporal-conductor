# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Temporal Conductor** executes [Netflix Conductor](https://conductor-oss.github.io/conductor/) workflow definitions using Temporal as the execution backend. A single Temporal workflow interprets a Conductor workflow definition at runtime (no code generation).

**Core pattern**: Reuse Conductor's libraries (DeciderService, expression evaluation, task mappers) with in-memory DAO implementations. Temporal provides durability via event history, eliminating need for external databases.

## Build & Test Commands

```bash
# Build and test
./gradlew build                    # Full build with tests
./gradlew test                     # Run all tests
./gradlew test --tests "*WorkflowTest*"   # Pattern matching
./gradlew bootRun                  # Run server (requires Temporal)
```

**Requirements**: JDK 21, Gradle wrapper included

## Project Structure

```
temporal-conductor/
├── build.gradle              # Root build config
├── settings.gradle           # Module configuration
├── temporal-conductor/       # Main module
│   ├── build.gradle
│   └── src/
│       ├── main/java/io/temporal/conductor/
│       │   ├── workflow/     # Temporal workflow implementations
│       │   ├── executor/     # DeciderService, SystemTaskExecutor
│       │   ├── activity/     # Temporal activities
│       │   ├── api/          # REST endpoints
│       │   ├── service/      # Service layer
│       │   ├── config/       # Spring configuration
│       │   └── dto/          # Data transfer objects
│       └── test/java/
└── design/                   # Design documents
```

## Key Files

- `workflow/ConductorWorkflowImpl.java` - Main Temporal workflow that interprets Conductor definitions
- `executor/TemporalDeciderServiceFactory.java` - Factory for DeciderService
- `executor/SystemTaskExecutor.java` - Executes FORK, JOIN, SWITCH, DO_WHILE
- `executor/InMemory*.java` - In-memory DAO implementations
- `api/*Resource.java` - REST endpoints
- `service/temporal/*Service.java` - Temporal backend services
- `config/TemporalConfig.java` - Spring/Temporal configuration

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

**Task type support**: SIMPLE, HTTP, FORK_JOIN, JOIN, SWITCH/DECISION, DO_WHILE, SET_VARIABLE, TERMINATE, WAIT all working.

## Running Locally

```bash
# Start Temporal and register all required search attributes
./scripts/start-temporal.sh

# Start the server with Temporal profile
SPRING_PROFILES_ACTIVE=temporal ./gradlew :temporal-conductor:bootRun

# Or use stub profile for testing without Temporal
SPRING_PROFILES_ACTIVE=stub ./gradlew :temporal-conductor:bootRun
```

**Swagger UI**: http://localhost:8080/swagger-ui.html
**Temporal UI**: http://localhost:8233

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
