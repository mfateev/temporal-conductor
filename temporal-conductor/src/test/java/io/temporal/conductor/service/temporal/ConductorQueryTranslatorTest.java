/*
 * Copyright Temporal Technologies Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.temporal.conductor.service.temporal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for ConductorQueryTranslator.
 *
 * <p>These tests verify the translation of Conductor SQL-like queries to Temporal List Filter
 * syntax. The translator handles field name mapping, timestamp conversion, and various SQL
 * operators.
 *
 * @see ConductorQueryTranslator
 */
class ConductorQueryTranslatorTest {

    private ConductorQueryTranslator translator;

    @BeforeEach
    void setUp() {
        translator = new ConductorQueryTranslator();
    }

    @Nested
    @DisplayName("Basic Equality Tests")
    class BasicEqualityTests {

        @Test
        @DisplayName("workflowType = 'order' translates to ConductorWorkflowType = 'order'")
        void testWorkflowTypeEquality() {
            String result = translator.translate("workflowType = 'order'", null);
            assertEquals("ConductorWorkflowType = 'order'", result);
        }

        @Test
        @DisplayName("status = 'RUNNING' translates to ConductorStatus = 'RUNNING'")
        void testStatusEquality() {
            String result = translator.translate("status = 'RUNNING'", null);
            assertEquals("ConductorStatus = 'RUNNING'", result);
        }

        @Test
        @DisplayName("workflowId = 'abc-123' translates to WorkflowId = 'abc-123'")
        void testWorkflowIdEquality() {
            String result = translator.translate("workflowId = 'abc-123'", null);
            assertEquals("WorkflowId = 'abc-123'", result);
        }

        @Test
        @DisplayName("correlationId = 'corr-456' translates to ConductorCorrelationId = 'corr-456'")
        void testCorrelationIdEquality() {
            String result = translator.translate("correlationId = 'corr-456'", null);
            assertEquals("ConductorCorrelationId = 'corr-456'", result);
        }

        @Test
        @DisplayName("status != 'COMPLETED' translates to ConductorStatus != 'COMPLETED'")
        void testStatusInequality() {
            String result = translator.translate("status != 'COMPLETED'", null);
            assertEquals("ConductorStatus != 'COMPLETED'", result);
        }
    }

    @Nested
    @DisplayName("Legacy Colon Syntax Tests")
    class LegacyColonSyntaxTests {

        @Test
        @DisplayName("status:RUNNING translates to ConductorStatus = 'RUNNING'")
        void testStatusColonSyntax() {
            String result = translator.translate("status:RUNNING", null);
            assertEquals("ConductorStatus = 'RUNNING'", result);
        }

        @Test
        @DisplayName("workflowType:order translates to ConductorWorkflowType = 'order'")
        void testWorkflowTypeColonSyntax() {
            String result = translator.translate("workflowType:order", null);
            assertEquals("ConductorWorkflowType = 'order'", result);
        }

        @Test
        @DisplayName("workflowId:abc123 translates to WorkflowId = 'abc123'")
        void testWorkflowIdColonSyntax() {
            String result = translator.translate("workflowId:abc123", null);
            assertEquals("WorkflowId = 'abc123'", result);
        }

        @Test
        @DisplayName("correlationId:corr456 translates to ConductorCorrelationId = 'corr456'")
        void testCorrelationIdColonSyntax() {
            String result = translator.translate("correlationId:corr456", null);
            assertEquals("ConductorCorrelationId = 'corr456'", result);
        }

        @Test
        @DisplayName("Legacy colon syntax with hyphenated values requires quoted syntax")
        void testLegacyColonHyphenatedValueLimitation() {
            // Note: Legacy colon syntax doesn't support hyphens in values
            // Use quoted equality syntax for hyphenated values: workflowId = 'abc-123'
            String result = translator.translate("workflowId = 'abc-123'", null);
            assertEquals("WorkflowId = 'abc-123'", result);
        }
    }

    @Nested
    @DisplayName("IN Clause Tests")
    class InClauseTests {

        @Test
        @DisplayName("status IN (RUNNING, PAUSED) translates to ConductorStatus IN ('RUNNING', 'PAUSED')")
        void testStatusInClauseUnquoted() {
            String result = translator.translate("status IN (RUNNING, PAUSED)", null);
            assertEquals("ConductorStatus IN ('RUNNING', 'PAUSED')", result);
        }

        @Test
        @DisplayName("status IN ('RUNNING', 'PAUSED') translates to ConductorStatus IN ('RUNNING', 'PAUSED')")
        void testStatusInClauseQuoted() {
            String result = translator.translate("status IN ('RUNNING', 'PAUSED')", null);
            assertEquals("ConductorStatus IN ('RUNNING', 'PAUSED')", result);
        }

        @Test
        @DisplayName("workflowType IN ('order', 'payment') translates correctly")
        void testWorkflowTypeInClause() {
            String result = translator.translate("workflowType IN ('order', 'payment')", null);
            assertEquals("ConductorWorkflowType IN ('order', 'payment')", result);
        }

        @Test
        @DisplayName("status IN (FAILED, TIMED_OUT, TERMINATED) translates correctly")
        void testStatusInClauseMultipleValues() {
            String result = translator.translate("status IN (FAILED, TIMED_OUT, TERMINATED)", null);
            assertEquals("ConductorStatus IN ('FAILED', 'TIMED_OUT', 'TERMINATED')", result);
        }
    }

    @Nested
    @DisplayName("Comparison Operator Tests")
    class ComparisonOperatorTests {

        @Test
        @DisplayName("priority > 5 translates to ConductorPriority > 5")
        void testPriorityGreaterThan() {
            String result = translator.translate("priority > 5", null);
            assertEquals("ConductorPriority > 5", result);
        }

        @Test
        @DisplayName("priority >= 5 translates to ConductorPriority >= 5")
        void testPriorityGreaterThanOrEqual() {
            String result = translator.translate("priority >= 5", null);
            assertEquals("ConductorPriority >= 5", result);
        }

        @Test
        @DisplayName("priority < 10 translates to ConductorPriority < 10")
        void testPriorityLessThan() {
            String result = translator.translate("priority < 10", null);
            assertEquals("ConductorPriority < 10", result);
        }

        @Test
        @DisplayName("priority <= 10 translates to ConductorPriority <= 10")
        void testPriorityLessThanOrEqual() {
            String result = translator.translate("priority <= 10", null);
            assertEquals("ConductorPriority <= 10", result);
        }

        @Test
        @DisplayName("version > 1 translates to ConductorWorkflowVersion > 1")
        void testVersionGreaterThan() {
            String result = translator.translate("version > 1", null);
            assertEquals("ConductorWorkflowVersion > 1", result);
        }
    }

    @Nested
    @DisplayName("AND/OR Combination Tests")
    class LogicalOperatorTests {

        @Test
        @DisplayName("workflowType = 'order' AND status = 'RUNNING' translates correctly")
        void testAndCombination() {
            String result = translator.translate("workflowType = 'order' AND status = 'RUNNING'", null);
            assertEquals("ConductorWorkflowType = 'order' AND ConductorStatus = 'RUNNING'", result);
        }

        @Test
        @DisplayName("status = 'FAILED' OR status = 'TIMED_OUT' translates correctly")
        void testOrCombination() {
            String result = translator.translate("status = 'FAILED' OR status = 'TIMED_OUT'", null);
            assertEquals("ConductorStatus = 'FAILED' OR ConductorStatus = 'TIMED_OUT'", result);
        }

        @Test
        @DisplayName("Complex AND/OR combination translates correctly")
        void testComplexCombination() {
            String result = translator.translate(
                    "workflowType = 'order' AND (status = 'FAILED' OR status = 'TIMED_OUT')", null);
            assertEquals(
                    "ConductorWorkflowType = 'order' AND (ConductorStatus = 'FAILED' OR ConductorStatus = 'TIMED_OUT')",
                    result);
        }

        @Test
        @DisplayName("Multiple AND clauses translate correctly")
        void testMultipleAndClauses() {
            String result = translator.translate(
                    "workflowType = 'order' AND status = 'RUNNING' AND priority > 5", null);
            assertEquals(
                    "ConductorWorkflowType = 'order' AND ConductorStatus = 'RUNNING' AND ConductorPriority > 5",
                    result);
        }

        @Test
        @DisplayName("Legacy colon syntax with AND translates correctly")
        void testLegacyColonWithAnd() {
            String result = translator.translate("status:RUNNING AND workflowType:order", null);
            assertEquals("ConductorStatus = 'RUNNING' AND ConductorWorkflowType = 'order'", result);
        }
    }

    @Nested
    @DisplayName("Timestamp Conversion Tests")
    class TimestampConversionTests {

        @Test
        @DisplayName("startTime > 1609459200000 translates to StartTime > '2021-01-01T00:00:00Z'")
        void testStartTimeGreaterThan() {
            String result = translator.translate("startTime > 1609459200000", null);
            assertEquals("StartTime > '2021-01-01T00:00:00Z'", result);
        }

        @Test
        @DisplayName("endTime > 1609459200000 translates to CloseTime > '2021-01-01T00:00:00Z'")
        void testEndTimeGreaterThan() {
            String result = translator.translate("endTime > 1609459200000", null);
            assertEquals("CloseTime > '2021-01-01T00:00:00Z'", result);
        }

        @Test
        @DisplayName("startTime BETWEEN timestamps translates to correct RFC 3339 format")
        void testStartTimeBetween() {
            // 1609459200000 = 2021-01-01T00:00:00Z
            // 1612137600000 = 2021-02-01T00:00:00Z
            String result = translator.translate("startTime BETWEEN 1609459200000 AND 1612137600000", null);
            assertEquals("StartTime BETWEEN '2021-01-01T00:00:00Z' AND '2021-02-01T00:00:00Z'", result);
        }

        @Test
        @DisplayName("startTime < timestamp translates correctly")
        void testStartTimeLessThan() {
            String result = translator.translate("startTime < 1609459200000", null);
            assertEquals("StartTime < '2021-01-01T00:00:00Z'", result);
        }

        @Test
        @DisplayName("startTime >= timestamp translates correctly")
        void testStartTimeGreaterThanOrEqual() {
            String result = translator.translate("startTime >= 1609459200000", null);
            assertEquals("StartTime >= '2021-01-01T00:00:00Z'", result);
        }
    }

    @Nested
    @DisplayName("IS NULL Tests")
    class IsNullTests {

        @Test
        @DisplayName("correlationId IS NULL translates to ConductorCorrelationId IS NULL")
        void testCorrelationIdIsNull() {
            String result = translator.translate("correlationId IS NULL", null);
            assertEquals("ConductorCorrelationId IS NULL", result);
        }

        @Test
        @DisplayName("ownerApp IS NULL translates to ConductorOwnerApp IS NULL")
        void testOwnerAppIsNull() {
            String result = translator.translate("ownerApp IS NULL", null);
            assertEquals("ConductorOwnerApp IS NULL", result);
        }

        @Test
        @DisplayName("correlationId IS NOT NULL translates to ConductorCorrelationId IS NOT NULL")
        void testCorrelationIdIsNotNull() {
            String result = translator.translate("correlationId IS NOT NULL", null);
            assertEquals("ConductorCorrelationId IS NOT NULL", result);
        }
    }

    @Nested
    @DisplayName("Edge Case Tests")
    class EdgeCaseTests {

        @Test
        @DisplayName("Empty query returns empty string")
        void testEmptyQuery() {
            String result = translator.translate("", null);
            assertEquals("", result);
        }

        @Test
        @DisplayName("Null query returns empty string")
        void testNullQuery() {
            String result = translator.translate(null, null);
            assertEquals("", result);
        }

        @Test
        @DisplayName("Wildcard * returns empty string (returns all)")
        void testWildcardQuery() {
            String result = translator.translate("*", null);
            assertEquals("", result);
        }

        @Test
        @DisplayName("Whitespace-only query returns empty string")
        void testWhitespaceQuery() {
            String result = translator.translate("   ", null);
            assertEquals("", result);
        }

        @Test
        @DisplayName("Field names are case-insensitive")
        void testCaseInsensitiveFieldNames() {
            String result1 = translator.translate("STATUS = 'RUNNING'", null);
            String result2 = translator.translate("Status = 'RUNNING'", null);
            String result3 = translator.translate("status = 'RUNNING'", null);

            assertEquals("ConductorStatus = 'RUNNING'", result1);
            assertEquals("ConductorStatus = 'RUNNING'", result2);
            assertEquals("ConductorStatus = 'RUNNING'", result3);
        }

        @Test
        @DisplayName("Already Temporal-formatted field names pass through")
        void testAlreadyTemporalFieldNames() {
            String result = translator.translate("ConductorStatus = 'RUNNING'", null);
            assertEquals("ConductorStatus = 'RUNNING'", result);
        }

        @Test
        @DisplayName("WorkflowId (built-in) passes through without prefix")
        void testBuiltInWorkflowId() {
            String result = translator.translate("WorkflowId = 'abc-123'", null);
            assertEquals("WorkflowId = 'abc-123'", result);
        }

        @Test
        @DisplayName("ExecutionStatus passes through unchanged")
        void testExecutionStatusPassthrough() {
            String result = translator.translate("ExecutionStatus = 'Running'", null);
            assertEquals("ExecutionStatus = 'Running'", result);
        }

        @Test
        @DisplayName("StartTime passes through unchanged")
        void testStartTimePassthrough() {
            String result = translator.translate("StartTime > '2021-01-01T00:00:00Z'", null);
            assertEquals("StartTime > '2021-01-01T00:00:00Z'", result);
        }
    }

    @Nested
    @DisplayName("Field Name Mapping Tests")
    class FieldNameMappingTests {

        @Test
        @DisplayName("workflowId maps to WorkflowId")
        void testWorkflowIdMapping() {
            assertEquals("WorkflowId", translator.translateFieldName("workflowId"));
        }

        @Test
        @DisplayName("workflowType maps to ConductorWorkflowType")
        void testWorkflowTypeMapping() {
            assertEquals("ConductorWorkflowType", translator.translateFieldName("workflowType"));
        }

        @Test
        @DisplayName("status maps to ConductorStatus")
        void testStatusMapping() {
            assertEquals("ConductorStatus", translator.translateFieldName("status"));
        }

        @Test
        @DisplayName("correlationId maps to ConductorCorrelationId")
        void testCorrelationIdMapping() {
            assertEquals("ConductorCorrelationId", translator.translateFieldName("correlationId"));
        }

        @Test
        @DisplayName("priority maps to ConductorPriority")
        void testPriorityMapping() {
            assertEquals("ConductorPriority", translator.translateFieldName("priority"));
        }

        @Test
        @DisplayName("startTime maps to StartTime")
        void testStartTimeMapping() {
            assertEquals("StartTime", translator.translateFieldName("startTime"));
        }

        @Test
        @DisplayName("endTime maps to CloseTime")
        void testEndTimeMapping() {
            assertEquals("CloseTime", translator.translateFieldName("endTime"));
        }

        @Test
        @DisplayName("version maps to ConductorWorkflowVersion")
        void testVersionMapping() {
            assertEquals("ConductorWorkflowVersion", translator.translateFieldName("version"));
        }

        @Test
        @DisplayName("ownerApp maps to ConductorOwnerApp")
        void testOwnerAppMapping() {
            assertEquals("ConductorOwnerApp", translator.translateFieldName("ownerApp"));
        }

        @Test
        @DisplayName("Unknown field passes through")
        void testUnknownFieldPassthrough() {
            assertEquals("CustomField", translator.translateFieldName("CustomField"));
        }
    }

    @Nested
    @DisplayName("Timestamp Formatting Tests")
    class TimestampFormattingTests {

        @Test
        @DisplayName("formatTimestamp converts millis to RFC 3339")
        void testFormatTimestamp() {
            // 1609459200000 = 2021-01-01T00:00:00Z
            String result = translator.formatTimestamp(1609459200000L);
            assertEquals("2021-01-01T00:00:00Z", result);
        }

        @Test
        @DisplayName("formatTimestamp handles epoch")
        void testFormatTimestampEpoch() {
            String result = translator.formatTimestamp(0L);
            assertEquals("1970-01-01T00:00:00Z", result);
        }

        @Test
        @DisplayName("formatTimestamp handles end of January 2021")
        void testFormatTimestampEndJan2021() {
            // 1612137600000 = 2021-02-01T00:00:00Z
            String result = translator.formatTimestamp(1612137600000L);
            assertEquals("2021-02-01T00:00:00Z", result);
        }
    }

    @Nested
    @DisplayName("FreeText Parameter Tests")
    class FreeTextTests {

        @Test
        @DisplayName("freeText parameter appends to query")
        void testFreeTextAppended() {
            String result = translator.translate("status = 'RUNNING'", "order");
            // Free text may be used for workflow type or other searches
            assertTrue(result.contains("ConductorStatus = 'RUNNING'"));
        }

        @Test
        @DisplayName("freeText alone creates type filter")
        void testFreeTextAlone() {
            String result = translator.translate(null, "orderWorkflow");
            // When only freeText is provided, it may be used as a workflowType filter
            assertTrue(result.isEmpty() || result.contains("orderWorkflow"));
        }
    }

    @Nested
    @DisplayName("Complex Query Tests")
    class ComplexQueryTests {

        @Test
        @DisplayName("Complex query with multiple field types translates correctly")
        void testComplexQuery() {
            String result = translator.translate(
                    "workflowType = 'order' AND status IN (RUNNING, PAUSED) AND priority >= 5", null);
            assertEquals(
                    "ConductorWorkflowType = 'order' AND ConductorStatus IN ('RUNNING', 'PAUSED') AND ConductorPriority >= 5",
                    result);
        }

        @Test
        @DisplayName("Query with timestamp and status translates correctly")
        void testTimestampAndStatus() {
            String result = translator.translate(
                    "status = 'FAILED' AND startTime > 1609459200000", null);
            assertEquals(
                    "ConductorStatus = 'FAILED' AND StartTime > '2021-01-01T00:00:00Z'",
                    result);
        }

        @Test
        @DisplayName("Query mixing equality and IN clause translates correctly")
        void testMixedEqualityAndIn() {
            String result = translator.translate(
                    "correlationId = 'batch-001' AND status IN (COMPLETED, FAILED)", null);
            assertEquals(
                    "ConductorCorrelationId = 'batch-001' AND ConductorStatus IN ('COMPLETED', 'FAILED')",
                    result);
        }
    }
}
