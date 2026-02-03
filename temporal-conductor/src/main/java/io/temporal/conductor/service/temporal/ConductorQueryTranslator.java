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

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Translates Conductor SQL-like queries to Temporal List Filter syntax.
 *
 * <p>Conductor queries use SQL-like syntax with field names like 'workflowType', 'status', etc.
 * This class translates them to Temporal's query format with proper field mapping,
 * timestamp conversion, and value quoting.
 *
 * <p>Supported query patterns:
 * <ul>
 *   <li>field = 'value' (equality)</li>
 *   <li>field != 'value' (inequality)</li>
 *   <li>field &gt; value, field &gt;= value, field &lt; value, field &lt;= value (comparison)</li>
 *   <li>field IN (v1, v2, v3) (set membership)</li>
 *   <li>field BETWEEN v1 AND v2 (range)</li>
 *   <li>field:value (legacy colon syntax)</li>
 *   <li>AND, OR logical operators</li>
 *   <li>Parentheses for grouping</li>
 *   <li>IS NULL, IS NOT NULL</li>
 * </ul>
 */
@Component
public class ConductorQueryTranslator {

    private static final Logger logger = LoggerFactory.getLogger(ConductorQueryTranslator.class);

    /** Fields that contain timestamp values in milliseconds. */
    private static final Set<String> TIMESTAMP_FIELDS = Set.of(
            "starttime", "endtime", "createtime", "updatetime");

    /**
     * Translate a Conductor query to Temporal List Filter syntax.
     *
     * @param conductorQuery Conductor SQL-like query
     * @param freeText Optional free-text search (currently not supported, ignored)
     * @return Temporal List Filter query string
     */
    public String translate(String conductorQuery, String freeText) {
        if (conductorQuery == null || conductorQuery.trim().isEmpty() || "*".equals(conductorQuery.trim())) {
            logger.debug("Empty or wildcard query, returning empty Temporal query");
            return "";
        }

        try {
            List<Token> tokens = tokenize(conductorQuery);
            String result = parse(tokens);
            logger.debug("Translated query: '{}' -> '{}'", conductorQuery, result);
            return result;
        } catch (QueryParseException e) {
            logger.warn("Failed to parse Conductor query '{}': {}", conductorQuery, e.getMessage());
            return "";
        } catch (Exception e) {
            logger.error("Unexpected error translating query '{}': {}", conductorQuery, e.getMessage(), e);
            return "";
        }
    }

    /**
     * Translate a single field name from Conductor to Temporal.
     *
     * @param conductorField the Conductor field name (case-insensitive)
     * @return the corresponding Temporal search attribute name
     */
    String translateFieldName(String conductorField) {
        return switch (conductorField.toLowerCase(Locale.ROOT)) {
            case "workflowid" -> "WorkflowId";
            case "workflowtype" -> "ConductorWorkflowType";
            case "status" -> "ConductorStatus";
            case "correlationid" -> "ConductorCorrelationId";
            case "priority" -> "ConductorPriority";
            case "starttime" -> "StartTime";
            case "endtime" -> "CloseTime";
            case "createtime" -> "StartTime";
            case "updatetime" -> "CloseTime";
            case "version" -> "ConductorWorkflowVersion";
            case "ownerapp" -> "ConductorOwnerApp";
            case "failedtasknames" -> "ConductorFailedTaskNames";
            default -> conductorField; // Pass through if already Temporal format
        };
    }

    /**
     * Convert timestamp (milliseconds since epoch) to RFC 3339 format.
     *
     * @param millis milliseconds since epoch
     * @return RFC 3339 formatted timestamp string
     */
    String formatTimestamp(long millis) {
        return Instant.ofEpochMilli(millis)
                .atOffset(ZoneOffset.UTC)
                .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    /**
     * Check if a field is a timestamp field.
     */
    private boolean isTimestampField(String fieldName) {
        return TIMESTAMP_FIELDS.contains(fieldName.toLowerCase(Locale.ROOT));
    }

    // ============== Tokenizer ==============

    /**
     * Token types for the query parser.
     */
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

    /**
     * A token from the query string.
     */
    static class Token {
        final TokenType type;
        final String value;
        final int position;

        Token(TokenType type, String value, int position) {
            this.type = type;
            this.value = value;
            this.position = position;
        }

        @Override
        public String toString() {
            return String.format("Token(%s, '%s', %d)", type, value, position);
        }
    }

    /**
     * Exception thrown when query parsing fails.
     */
    static class QueryParseException extends RuntimeException {
        QueryParseException(String message) {
            super(message);
        }

        QueryParseException(String message, int position) {
            super(message + " at position " + position);
        }
    }

    /**
     * Tokenize the input query string.
     */
    List<Token> tokenize(String input) {
        List<Token> tokens = new ArrayList<>();
        int pos = 0;
        int length = input.length();

        while (pos < length) {
            char c = input.charAt(pos);

            // Skip whitespace
            if (Character.isWhitespace(c)) {
                pos++;
                continue;
            }

            // Single-quoted string
            if (c == '\'') {
                int start = pos;
                pos++;
                StringBuilder sb = new StringBuilder();
                while (pos < length && input.charAt(pos) != '\'') {
                    if (input.charAt(pos) == '\\' && pos + 1 < length) {
                        pos++;
                        sb.append(input.charAt(pos));
                    } else {
                        sb.append(input.charAt(pos));
                    }
                    pos++;
                }
                if (pos >= length) {
                    throw new QueryParseException("Unterminated string literal", start);
                }
                pos++; // consume closing quote
                tokens.add(new Token(TokenType.STRING, sb.toString(), start));
                continue;
            }

            // Parentheses
            if (c == '(') {
                tokens.add(new Token(TokenType.LPAREN, "(", pos));
                pos++;
                continue;
            }
            if (c == ')') {
                tokens.add(new Token(TokenType.RPAREN, ")", pos));
                pos++;
                continue;
            }

            // Comma
            if (c == ',') {
                tokens.add(new Token(TokenType.COMMA, ",", pos));
                pos++;
                continue;
            }

            // Colon (legacy syntax)
            if (c == ':') {
                tokens.add(new Token(TokenType.COLON, ":", pos));
                pos++;
                continue;
            }

            // Operators: =, !=, <, >, <=, >=
            if (c == '=') {
                tokens.add(new Token(TokenType.OPERATOR, "=", pos));
                pos++;
                continue;
            }
            if (c == '!' && pos + 1 < length && input.charAt(pos + 1) == '=') {
                tokens.add(new Token(TokenType.OPERATOR, "!=", pos));
                pos += 2;
                continue;
            }
            if (c == '<') {
                if (pos + 1 < length && input.charAt(pos + 1) == '=') {
                    tokens.add(new Token(TokenType.OPERATOR, "<=", pos));
                    pos += 2;
                } else if (pos + 1 < length && input.charAt(pos + 1) == '>') {
                    tokens.add(new Token(TokenType.OPERATOR, "!=", pos));
                    pos += 2;
                } else {
                    tokens.add(new Token(TokenType.OPERATOR, "<", pos));
                    pos++;
                }
                continue;
            }
            if (c == '>') {
                if (pos + 1 < length && input.charAt(pos + 1) == '=') {
                    tokens.add(new Token(TokenType.OPERATOR, ">=", pos));
                    pos += 2;
                } else {
                    tokens.add(new Token(TokenType.OPERATOR, ">", pos));
                    pos++;
                }
                continue;
            }

            // Hyphen/minus - tokenize as operator only when NOT followed by a digit
            // (negative numbers are handled below)
            if (c == '-' && !(pos + 1 < length && Character.isDigit(input.charAt(pos + 1)))) {
                tokens.add(new Token(TokenType.OPERATOR, "-", pos));
                pos++;
                continue;
            }

            // Numbers (including negative)
            if (Character.isDigit(c) || (c == '-' && pos + 1 < length && Character.isDigit(input.charAt(pos + 1)))) {
                int start = pos;
                if (c == '-') pos++;
                while (pos < length && (Character.isDigit(input.charAt(pos)) || input.charAt(pos) == '.')) {
                    pos++;
                }
                tokens.add(new Token(TokenType.NUMBER, input.substring(start, pos), start));
                continue;
            }

            // Identifiers and keywords
            if (Character.isLetter(c) || c == '_') {
                int start = pos;
                while (pos < length && (Character.isLetterOrDigit(input.charAt(pos)) || input.charAt(pos) == '_')) {
                    pos++;
                }
                String word = input.substring(start, pos);
                String upper = word.toUpperCase(Locale.ROOT);

                // Check for keywords
                if (isKeyword(upper)) {
                    tokens.add(new Token(TokenType.KEYWORD, upper, start));
                } else {
                    tokens.add(new Token(TokenType.IDENTIFIER, word, start));
                }
                continue;
            }

            // Unknown character
            throw new QueryParseException("Unexpected character '" + c + "'", pos);
        }

        tokens.add(new Token(TokenType.EOF, "", pos));
        return tokens;
    }

    private boolean isKeyword(String word) {
        return switch (word) {
            case "AND", "OR", "IN", "BETWEEN", "IS", "NULL", "NOT" -> true;
            default -> false;
        };
    }

    // ============== Parser ==============

    /**
     * Parse tokens and generate Temporal query.
     */
    private String parse(List<Token> tokens) {
        Parser parser = new Parser(tokens);
        return parser.parseExpression();
    }

    /**
     * Recursive descent parser for the query grammar.
     */
    private class Parser {
        private final List<Token> tokens;
        private int pos;

        Parser(List<Token> tokens) {
            this.tokens = tokens;
            this.pos = 0;
        }

        Token current() {
            return tokens.get(pos);
        }

        Token consume() {
            return tokens.get(pos++);
        }

        void expect(TokenType type, String value) {
            Token t = current();
            if (t.type != type || (value != null && !value.equals(t.value))) {
                throw new QueryParseException(
                        "Expected " + type + (value != null ? " '" + value + "'" : "") + ", got " + t, t.position);
            }
            consume();
        }

        boolean match(TokenType type) {
            return current().type == type;
        }

        boolean match(TokenType type, String value) {
            Token t = current();
            return t.type == type && value.equals(t.value);
        }

        /**
         * Parse a full expression (handles OR at the lowest precedence).
         */
        String parseExpression() {
            return parseOr();
        }

        String parseOr() {
            StringBuilder result = new StringBuilder();
            result.append(parseAnd());

            while (match(TokenType.KEYWORD, "OR")) {
                consume();
                result.append(" OR ");
                result.append(parseAnd());
            }

            return result.toString();
        }

        String parseAnd() {
            StringBuilder result = new StringBuilder();
            result.append(parsePrimary());

            while (match(TokenType.KEYWORD, "AND")) {
                consume();
                result.append(" AND ");
                result.append(parsePrimary());
            }

            return result.toString();
        }

        String parsePrimary() {
            // Handle parentheses
            if (match(TokenType.LPAREN)) {
                consume();
                String inner = parseExpression();
                expect(TokenType.RPAREN, null);
                return "(" + inner + ")";
            }

            // Handle clause (field operator value)
            return parseClause();
        }

        String parseClause() {
            Token fieldToken = current();
            if (fieldToken.type != TokenType.IDENTIFIER) {
                throw new QueryParseException("Expected field name", fieldToken.position);
            }
            consume();

            String originalField = fieldToken.value;
            String temporalField = translateFieldName(originalField);

            Token next = current();

            // Handle legacy colon syntax: field:value
            // This syntax supports alphanumeric values with hyphens and underscores
            if (next.type == TokenType.COLON) {
                consume();
                String value = parseLegacyColonValue(originalField);
                return temporalField + " = " + value;
            }

            // Handle IS NULL / IS NOT NULL
            if (match(TokenType.KEYWORD, "IS")) {
                consume();
                boolean negated = false;
                if (match(TokenType.KEYWORD, "NOT")) {
                    consume();
                    negated = true;
                }
                expect(TokenType.KEYWORD, "NULL");
                return temporalField + (negated ? " IS NOT NULL" : " IS NULL");
            }

            // Handle IN clause
            if (match(TokenType.KEYWORD, "IN")) {
                consume();
                expect(TokenType.LPAREN, null);
                List<String> values = new ArrayList<>();
                values.add(parseValue(originalField));
                while (match(TokenType.COMMA)) {
                    consume();
                    values.add(parseValue(originalField));
                }
                expect(TokenType.RPAREN, null);
                return temporalField + " IN (" + String.join(", ", values) + ")";
            }

            // Handle BETWEEN clause
            if (match(TokenType.KEYWORD, "BETWEEN")) {
                consume();
                String low = parseValue(originalField);
                expect(TokenType.KEYWORD, "AND");
                String high = parseValue(originalField);
                return temporalField + " BETWEEN " + low + " AND " + high;
            }

            // Handle comparison operators
            if (next.type == TokenType.OPERATOR) {
                String op = consume().value;
                String value = parseValue(originalField);
                return temporalField + " " + op + " " + value;
            }

            throw new QueryParseException("Expected operator after field name", next.position);
        }

        /**
         * Parse a value in legacy colon syntax (field:value).
         * This format supports identifiers with hyphens and underscores.
         * It consumes consecutive identifiers, numbers, and hyphens to form the value.
         */
        String parseLegacyColonValue(String fieldName) {
            StringBuilder valueBuilder = new StringBuilder();
            Token t = current();

            // If it's a quoted string, use normal parsing
            if (t.type == TokenType.STRING) {
                consume();
                return "'" + t.value + "'";
            }

            // Build value from consecutive tokens until we hit AND/OR/EOF/RPAREN
            while (t.type == TokenType.IDENTIFIER || t.type == TokenType.NUMBER) {
                valueBuilder.append(t.value);
                consume();
                t = current();

                // Check if next is a hyphen followed by more identifier/number
                // We need to peek ahead to handle abc-123 style values
                if (t.type == TokenType.OPERATOR && "-".equals(t.value)) {
                    Token afterHyphen = tokens.get(pos + 1);
                    if (afterHyphen.type == TokenType.NUMBER || afterHyphen.type == TokenType.IDENTIFIER) {
                        valueBuilder.append("-");
                        consume(); // consume the hyphen
                        t = current();
                        continue;
                    }
                }

                // Also handle negative numbers that were tokenized separately
                if (t.type == TokenType.NUMBER && t.value.startsWith("-")) {
                    valueBuilder.append(t.value);
                    consume();
                    t = current();
                    continue;
                }

                break;
            }

            if (valueBuilder.length() == 0) {
                throw new QueryParseException("Expected value after colon", t.position);
            }

            String value = valueBuilder.toString();

            // Check if it's a timestamp field
            if (isTimestampField(fieldName)) {
                try {
                    long millis = Long.parseLong(value);
                    return "'" + formatTimestamp(millis) + "'";
                } catch (NumberFormatException e) {
                    // Not a valid timestamp, use as-is
                }
            }

            return "'" + value + "'";
        }

        /**
         * Parse a value, handling quoting and timestamp conversion.
         */
        String parseValue(String fieldName) {
            Token t = current();

            // String literal - already quoted
            if (t.type == TokenType.STRING) {
                consume();
                return "'" + t.value + "'";
            }

            // Number - might need timestamp conversion
            if (t.type == TokenType.NUMBER) {
                consume();
                if (isTimestampField(fieldName)) {
                    try {
                        long millis = Long.parseLong(t.value);
                        return "'" + formatTimestamp(millis) + "'";
                    } catch (NumberFormatException e) {
                        // Not a valid timestamp, use as-is
                        return t.value;
                    }
                }
                return t.value;
            }

            // Identifier (unquoted value) - quote it
            if (t.type == TokenType.IDENTIFIER) {
                consume();
                // Check if it's a keyword that should remain unquoted (like NULL)
                if (isKeyword(t.value.toUpperCase(Locale.ROOT))) {
                    return t.value.toUpperCase(Locale.ROOT);
                }
                return "'" + t.value + "'";
            }

            throw new QueryParseException("Expected value", t.position);
        }
    }
}
