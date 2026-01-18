package io.temporal.conductor.poc;

import com.netflix.conductor.core.execution.tasks.SystemTaskRegistry;
import com.netflix.conductor.core.execution.tasks.WorkflowSystemTask;
import java.util.Collections;

/** Minimal implementation of SystemTaskRegistry for POC testing (no system tasks registered). */
public class MinimalSystemTaskRegistry extends SystemTaskRegistry {

  public MinimalSystemTaskRegistry() {
    super(Collections.emptySet());
  }

  @Override
  public boolean isSystemTask(String taskType) {
    // For POC, we only test SIMPLE tasks (not system tasks)
    return false;
  }
}
