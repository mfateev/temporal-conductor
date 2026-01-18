package io.temporal.conductor.poc;

import com.netflix.conductor.core.utils.IDGenerator;
import java.util.UUID;

/** Minimal implementation of IDGenerator for POC testing. */
public class MinimalIDGenerator extends IDGenerator {

  @Override
  public String generate() {
    return UUID.randomUUID().toString();
  }
}
