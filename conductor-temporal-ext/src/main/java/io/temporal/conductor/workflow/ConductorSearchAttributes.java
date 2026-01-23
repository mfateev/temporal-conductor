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

package io.temporal.conductor.workflow;

import io.temporal.common.SearchAttributeKey;
import java.util.List;

/**
 * Search attribute keys for Conductor workflows running on Temporal.
 *
 * <p>These attributes enable visibility queries for workflow listing and filtering in the
 * Conductor UI when backed by Temporal.
 *
 * <p>Registration script:
 * <pre>
 * temporal operator search-attribute create \
 *   --namespace conductor \
 *   --name ConductorWorkflowType --type Keyword \
 *   --name ConductorWorkflowVersion --type Int \
 *   --name ConductorStatus --type Keyword \
 *   --name ConductorCorrelationId --type Keyword \
 *   --name ConductorPriority --type Int \
 *   --name ConductorFailedTaskNames --type KeywordList \
 *   --name ConductorOwnerApp --type Keyword
 * </pre>
 */
public final class ConductorSearchAttributes {

    /** Filter by Conductor workflow type (workflow definition name). */
    public static final SearchAttributeKey<String> WORKFLOW_TYPE =
            SearchAttributeKey.forKeyword("ConductorWorkflowType");

    /** Filter by Conductor workflow definition version. */
    public static final SearchAttributeKey<Long> WORKFLOW_VERSION =
            SearchAttributeKey.forLong("ConductorWorkflowVersion");

    /** Filter by Conductor workflow status (RUNNING, COMPLETED, FAILED, etc.). */
    public static final SearchAttributeKey<String> STATUS =
            SearchAttributeKey.forKeyword("ConductorStatus");

    /** Filter by correlation ID to group related workflows. */
    public static final SearchAttributeKey<String> CORRELATION_ID =
            SearchAttributeKey.forKeyword("ConductorCorrelationId");

    /** Sort or filter by workflow priority. */
    public static final SearchAttributeKey<Long> PRIORITY =
            SearchAttributeKey.forLong("ConductorPriority");

    /** Filter by failed task reference names. */
    public static final SearchAttributeKey<List<String>> FAILED_TASK_NAMES =
            SearchAttributeKey.forKeywordList("ConductorFailedTaskNames");

    /** Filter by owner application for multi-tenant scenarios. */
    public static final SearchAttributeKey<String> OWNER_APP =
            SearchAttributeKey.forKeyword("ConductorOwnerApp");

    private ConductorSearchAttributes() {
        // Prevent instantiation
    }
}
