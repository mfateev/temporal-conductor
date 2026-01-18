package io.temporal.conductor.poc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.utils.ExternalPayloadStorage;
import com.netflix.conductor.core.config.ConductorProperties;
import com.netflix.conductor.core.utils.ExternalPayloadStorageUtils;

/** Minimal implementation of ExternalPayloadStorageUtils for POC testing (no external storage). */
public class MinimalExternalPayloadStorageUtils extends ExternalPayloadStorageUtils {

  public MinimalExternalPayloadStorageUtils(
      ExternalPayloadStorage externalPayloadStorage,
      ConductorProperties properties,
      ObjectMapper objectMapper) {
    super(externalPayloadStorage, properties, objectMapper);
  }

  // Uses default implementations from parent class
  // For POC, we won't use external payload storage
}
