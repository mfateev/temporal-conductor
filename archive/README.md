# Archived POC Modules

These modules contain the proof-of-concept implementations that validated the core approach before productization.

## POC 1: DeciderService Isolation
**Status:** PASSED
**Location:** `poc-1-decider-isolation/`

Validated that Conductor's DeciderService can run standalone with HashMap-based DAOs, without requiring external databases.

## POC 2: Determinism Audit
**Status:** PASSED (with mitigations)
**Location:** `poc-2-determinism-audit/`

Identified determinism issues in Conductor's DeciderService (UUID.randomUUID(), System.currentTimeMillis()) and developed mitigations for Temporal replay safety.

## POC 3: Temporal Workflow Integration
**Status:** PASSED
**Location:** `poc-3-temporal-integration/`

Full Kotlin implementation of Conductor workflow execution on Temporal. Supports all critical task types: SIMPLE, HTTP, FORK_JOIN, JOIN, SWITCH, DO_WHILE, SET_VARIABLE, TERMINATE.

---

**Note:** The production implementation is now in:
- `conductor-temporal-ext/` - Core library (Java)
- `conductor-server-temporal/` - Spring Boot REST server (Java)
