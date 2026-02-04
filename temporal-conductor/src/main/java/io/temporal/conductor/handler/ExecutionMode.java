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

package io.temporal.conductor.handler;

/**
 * Defines how a task type should be executed within the Temporal workflow.
 *
 * <p>Each execution mode maps to a specific Temporal primitive:
 * <ul>
 *   <li>{@link #SYNC} - Execute synchronously in workflow (e.g., FORK, JOIN, SWITCH)</li>
 *   <li>{@link #ACTIVITY} - Execute via Temporal activity (e.g., SIMPLE, HTTP, extensions)</li>
 *   <li>{@link #TIMER} - Wait for Temporal timer (e.g., WAIT with duration)</li>
 *   <li>{@link #WAIT_FOR_SIGNAL} - Wait for external signal (e.g., HUMAN, WAIT with no duration)</li>
 *   <li>{@link #CHILD_WORKFLOW} - Start child workflow and wait (e.g., SUB_WORKFLOW)</li>
 *   <li>{@link #FIRE_AND_FORGET_CHILD} - Start child workflow without waiting (e.g., START_WORKFLOW)</li>
 *   <li>{@link #ASYNC_REEVAL} - System task needing re-evaluation (e.g., DO_WHILE, JOIN)</li>
 * </ul>
 */
public enum ExecutionMode {

    /**
     * Execute synchronously within the workflow.
     * Used for tasks like FORK, JOIN, SWITCH, SET_VARIABLE, TERMINATE.
     * These tasks complete immediately without blocking.
     */
    SYNC,

    /**
     * Execute via Temporal activity.
     * Used for SIMPLE, HTTP, EVENT, and extension tasks.
     * The task is scheduled as an activity and the workflow waits for completion.
     */
    ACTIVITY,

    /**
     * Wait for a Temporal timer.
     * Used for WAIT tasks with a specified duration.
     * The task waits for the timer to fire before completing.
     */
    TIMER,

    /**
     * Wait for an external signal to complete the task.
     * Used for HUMAN tasks and WAIT tasks without a duration.
     * The task waits indefinitely until a signal is received.
     */
    WAIT_FOR_SIGNAL,

    /**
     * Start a child workflow and wait for its completion.
     * Used for SUB_WORKFLOW tasks.
     * The parent workflow waits for the child to complete.
     */
    CHILD_WORKFLOW,

    /**
     * Start a child workflow without waiting for completion.
     * Used for START_WORKFLOW tasks (fire-and-forget pattern).
     * The task completes immediately after starting the child.
     */
    FIRE_AND_FORGET_CHILD,

    /**
     * System task that requires re-evaluation on each scheduling loop iteration.
     * Used for DO_WHILE (loop control) and JOIN (waiting for forked branches).
     * These tasks may not complete on first execution and need periodic re-checks.
     */
    ASYNC_REEVAL
}
