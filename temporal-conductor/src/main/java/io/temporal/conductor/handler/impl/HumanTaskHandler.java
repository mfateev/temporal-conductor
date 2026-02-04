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

package io.temporal.conductor.handler.impl;

import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import io.temporal.conductor.handler.ExecutionMode;
import io.temporal.conductor.handler.TaskExecutionContext;
import io.temporal.conductor.handler.TaskTypeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for HUMAN tasks that wait for manual completion.
 *
 * <p>HUMAN tasks wait indefinitely for external completion via signal.
 * They are completed by calling the completeTask signal with the task
 * reference name and output data.
 *
 * <p>Use case: Tasks requiring human review, approval, or data entry.
 */
public class HumanTaskHandler implements TaskTypeHandler {

    private static final Logger logger = LoggerFactory.getLogger(HumanTaskHandler.class);

    @Override
    public String getTaskType() {
        return TaskType.HUMAN.name();
    }

    @Override
    public ExecutionMode getExecutionMode(TaskModel task) {
        return ExecutionMode.WAIT_FOR_SIGNAL;
    }

    @Override
    public void execute(TaskModel task, WorkflowModel workflow, TaskExecutionContext context) {
        String taskRefName = task.getReferenceTaskName();

        if (task.getStatus() == TaskModel.Status.SCHEDULED) {
            task.setStatus(TaskModel.Status.IN_PROGRESS);
        }

        if (task.getStatus().isTerminal()) {
            if (task.getEndTime() == 0L) {
                task.setEndTime(context.currentTimeMillis());
            }
            return;
        }

        // HUMAN tasks wait indefinitely for a completeTask signal
        // The signal handler in ConductorWorkflowImpl will complete the task
        logger.info("HUMAN task {} waiting for manual completion via signal", taskRefName);
    }
}
