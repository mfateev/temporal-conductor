# Query Translation & Visibility Integration Plan

## Overview

This plan details the implementation of Conductor-to-Temporal query translation, enabling full workflow search functionality through Temporal's Visibility API.

## Current State

### Current Implementation (`TemporalWorkflowService.buildTemporalQuery`)

The current implementation handles only basic colon-separated queries:

```java
// Input:  "status:RUNNING AND workflowType:myWorkflow"
// Output: "ConductorStatus = 'RUNNING' AND WorkflowType = 'myWorkflow'"
```

**Limitations:**
- Only supports `:` as key-value separator
- No support for operators (`=`, `!=`, `>`, `<`, `IN`, `BETWEEN`)
- No support for quoted strings
- No support for `OR` conditions
- No proper SQL-like parsing

### Registered Search Attributes

From `scripts/start-temporal.sh`:

| Search Attribute | Type | Purpose |
|-----------------|------|---------|
| ConductorWorkflowType | Keyword | Conductor workflow definition name |
| ConductorWorkflowVersion | Int | Conductor workflow version |
| ConductorStatus | Keyword | Conductor status (RUNNING, PAUSED, COMPLETED, etc.) |
| ConductorCorrelationId | Keyword | Correlation ID for workflow grouping |
| ConductorPriority | Int | Workflow priority |
| ConductorFailedTaskNames | KeywordList | Names of failed tasks |
| ConductorOwnerApp | Keyword | Application that owns the workflow |

Plus Temporal built-in attributes:
- `WorkflowId` - Workflow instance ID
- `WorkflowType` - Temporal workflow type (Conductor workflow name)
- `ExecutionStatus` - Temporal execution status
- `StartTime` - Workflow start time
- `CloseTime` - Workflow close/end time
- `RunId` - Workflow run ID

---

## Conductor Query Syntax

Conductor supports SQL-like queries with these patterns:

```sql
-- Basic equality
workflowType = 'orderWorkflow'
status = 'RUNNING'

-- Inequality
status != 'COMPLETED'

-- IN clause
status IN (RUNNING, PAUSED)
workflowType IN ('order', 'payment')

-- Comparison operators (for dates, numbers)
startTime > 1609459200000
priority >= 5

-- BETWEEN (for time ranges)
startTime BETWEEN 1609459200000 AND 1612137600000

-- AND/OR combinations
workflowType = 'order' AND status = 'RUNNING'
status = 'FAILED' OR status = 'TIMED_OUT'

-- Free text (Elasticsearch style)
*:value
workflowType:order*
```

---

## Temporal List Filter Syntax

From [Temporal Visibility Documentation](https://docs.temporal.io/visibility#list-filter):

```sql
-- Supported operators
=, !=, >, >=, <, <=
AND, OR (AND has higher precedence)
IN
BETWEEN ... AND
STARTS_WITH (for Keyword)
IS NULL, IS NOT NULL

-- String values must be single-quoted
WorkflowType = 'OrderWorkflow'

-- Datetime format: RFC 3339 with timezone
StartTime > '2024-01-01T00:00:00Z'

-- ORDER BY (default: StartTime DESC, WorkflowId)
ORDER BY StartTime ASC

-- IS NULL for unset search attributes
ConductorCorrelationId IS NULL
```

**Important Notes:**
- Search attribute names are **case-sensitive**
- String values must use **single quotes**
- DateTime uses **RFC 3339** format (ISO 8601 with timezone)
- `ORDER BY` only supports StartTime and CloseTime
- Parentheses supported for grouping

---

## Field Mapping

### Conductor → Temporal Field Mapping

| Conductor Field | Temporal Search Attribute | Notes |
|----------------|---------------------------|-------|
| `workflowId` | `WorkflowId` | Built-in |
| `workflowType` | `ConductorWorkflowType` | Custom attribute |
| `status` | `ConductorStatus` | Custom attribute (Conductor status values) |
| `correlationId` | `ConductorCorrelationId` | Custom attribute |
| `priority` | `ConductorPriority` | Custom attribute |
| `startTime` | `StartTime` | Built-in (convert millis to RFC 3339) |
| `endTime` | `CloseTime` | Built-in (convert millis to RFC 3339) |
| `version` | `ConductorWorkflowVersion` | Custom attribute |
| `ownerApp` | `ConductorOwnerApp` | Custom attribute |
| `failedTaskNames` | `ConductorFailedTaskNames` | Custom attribute (KeywordList) |

### Status Value Mapping

Conductor status values map directly to `ConductorStatus` search attribute:

| Conductor Status | ConductorStatus Value |
|-----------------|----------------------|
| RUNNING | RUNNING |
| PAUSED | PAUSED |
| COMPLETED | COMPLETED |
| FAILED | FAILED |
| TIMED_OUT | TIMED_OUT |
| TERMINATED | TERMINATED |

**Note:** The workflow sets `ConductorStatus` search attribute, so we query it directly without mapping to Temporal's `ExecutionStatus`.

---

## Implementation Plan

### Phase 1: ConductorQueryTranslator Class

Create a dedicated query translator class:

```
temporal-conductor/src/main/java/io/temporal/conductor/service/temporal/
└── ConductorQueryTranslator.java
```

#### Class Design

```java
public class ConductorQueryTranslator {

    /**
     * Translate a Conductor query to Temporal List Filter syntax.
     *
     * @param conductorQuery Conductor SQL-like query (e.g., "workflowType = 'order' AND status IN (RUNNING, PAUSED)")
     * @param freeText Optional free-text search (Elasticsearch-style)
     * @return Temporal List Filter query string
     */
    public String translate(String conductorQuery, String freeText) {
        // 1. Handle empty/null input
        // 2. Tokenize the query
        // 3. Parse and translate each clause
        // 4. Handle free-text search
        // 5. Return Temporal-compatible query
    }

    /**
     * Translate a single field name from Conductor to Temporal.
     */
    String translateFieldName(String conductorField) {
        return switch (conductorField.toLowerCase()) {
            case "workflowid" -> "WorkflowId";
            case "workflowtype" -> "ConductorWorkflowType";
            case "status" -> "ConductorStatus";
            case "correlationid" -> "ConductorCorrelationId";
            case "priority" -> "ConductorPriority";
            case "starttime" -> "StartTime";
            case "endtime" -> "CloseTime";
            case "version" -> "ConductorWorkflowVersion";
            case "ownerapp" -> "ConductorOwnerApp";
            default -> conductorField; // Pass through if already Temporal format
        };
    }

    /**
     * Convert timestamp (millis) to RFC 3339 format.
     */
    String formatTimestamp(long millis) {
        return Instant.ofEpochMilli(millis)
            .atOffset(ZoneOffset.UTC)
            .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }
}
```

### Phase 2: Query Parsing Strategy

Use a simple tokenizer/parser approach (no external dependencies):

1. **Tokenize** the input into tokens:
   - Identifiers (field names)
   - Operators (`=`, `!=`, `>`, `<`, `>=`, `<=`, `IN`, `BETWEEN`)
   - Logical operators (`AND`, `OR`)
   - Literals (strings in quotes, numbers)
   - Parentheses, commas

2. **Parse** tokens into AST or direct translation:
   - Clause: `field operator value`
   - Compound: `clause (AND|OR) clause`

3. **Translate** each clause:
   - Map field names
   - Convert timestamps
   - Ensure proper quoting

#### Token Types

```java
enum TokenType {
    IDENTIFIER,    // field names
    STRING,        // 'quoted string'
    NUMBER,        // 123, 123.45
    OPERATOR,      // =, !=, >, <, >=, <=
    KEYWORD,       // AND, OR, IN, BETWEEN, IS, NULL, NOT
    LPAREN,        // (
    RPAREN,        // )
    COMMA,         // ,
    COLON,         // : (for legacy key:value syntax)
    EOF
}
```

### Phase 3: Test Cases

Create comprehensive unit tests:

```
temporal-conductor/src/test/java/io/temporal/conductor/service/temporal/
└── ConductorQueryTranslatorTest.java
```

#### Test Cases

| Input (Conductor) | Expected Output (Temporal) |
|-------------------|---------------------------|
| `workflowType = 'order'` | `ConductorWorkflowType = 'order'` |
| `status = 'RUNNING'` | `ConductorStatus = 'RUNNING'` |
| `status IN (RUNNING, PAUSED)` | `ConductorStatus IN ('RUNNING', 'PAUSED')` |
| `workflowType = 'order' AND status = 'RUNNING'` | `ConductorWorkflowType = 'order' AND ConductorStatus = 'RUNNING'` |
| `status:RUNNING` (legacy) | `ConductorStatus = 'RUNNING'` |
| `priority > 5` | `ConductorPriority > 5` |
| `startTime > 1609459200000` | `StartTime > '2021-01-01T00:00:00Z'` |
| `startTime BETWEEN 1609459200000 AND 1612137600000` | `StartTime BETWEEN '2021-01-01T00:00:00Z' AND '2021-02-01T00:00:00Z'` |
| `*` or empty | `` (empty - returns all) |
| `correlationId IS NULL` | `ConductorCorrelationId IS NULL` |

### Phase 4: Integration

Update `TemporalWorkflowService.buildTemporalQuery` to use the new translator:

```java
private final ConductorQueryTranslator queryTranslator = new ConductorQueryTranslator();

private String buildTemporalQuery(String conductorQuery, String freeText) {
    return queryTranslator.translate(conductorQuery, freeText);
}
```

---

## Implementation Steps

### Step 1: Create ConductorQueryTranslator

1. Create `ConductorQueryTranslator.java` with:
   - Field name mapping
   - Timestamp conversion
   - Basic tokenizer
   - Clause parser
   - Query builder

2. Support these query patterns:
   - `field = 'value'` (equality)
   - `field != 'value'` (inequality)
   - `field > value` (comparison)
   - `field IN (v1, v2, v3)` (set membership)
   - `field BETWEEN v1 AND v2` (range)
   - `field:value` (legacy colon syntax)
   - `AND`, `OR` logical operators
   - Parentheses for grouping

### Step 2: Create Unit Tests

1. Create `ConductorQueryTranslatorTest.java`
2. Test all translation scenarios
3. Test edge cases (empty, null, malformed)
4. Test timestamp conversion
5. Test case insensitivity for field names

### Step 3: Update TemporalWorkflowService

1. Replace `buildTemporalQuery` implementation
2. Add error handling for invalid queries
3. Log translated queries for debugging

### Step 4: Verify Search Attribute Updates ✅ ALREADY IMPLEMENTED

The workflow already sets search attributes correctly:

- `ConductorSearchAttributes.java` - Defines all search attribute keys
- `ConductorWorkflowImpl.initializeSearchAttributes()` - Sets attributes on workflow start
- `ConductorWorkflowImpl.updateStatusAttribute()` - Updates status when workflow status changes
- `ConductorWorkflowImpl.updateFailedTaskNamesAttribute()` - Updates failed task names list

**No changes needed** - the infrastructure is already in place.

### Step 5: E2E Tests

Add E2E tests for search functionality:

1. Start workflows with different attributes
2. Search using various query patterns
3. Verify results match expected workflows

---

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `service/temporal/ConductorQueryTranslator.java` | CREATE | Query translation logic |
| `service/temporal/ConductorQueryTranslatorTest.java` | CREATE | Unit tests for translator |
| `service/temporal/TemporalWorkflowService.java` | MODIFY | Use new translator |
| `workflow/ConductorWorkflowImpl.java` | VERIFY | Ensure search attributes set |

---

## Verification

```bash
# Run unit tests
./gradlew :temporal-conductor:test --tests "*ConductorQueryTranslator*"

# Run all unit tests
./gradlew :temporal-conductor:test

# Run E2E tests
./gradlew :e2e-tests:test
```

---

## Future Enhancements

1. **STARTS_WITH support**: For prefix matching on workflow types
2. **ORDER BY support**: For sorted results
3. **COUNT queries**: For workflow counts by criteria
4. **Pagination**: Handle `nextPageToken` for large result sets
5. **Query validation**: Validate queries before execution

---

## References

- [Temporal Visibility Documentation](https://docs.temporal.io/visibility)
- [Temporal List Filter](https://docs.temporal.io/visibility#list-filter)
- [Conductor Workflow Search API](https://conductor-oss.github.io/conductor/devguide/how-tos/searching-workflows.html)
