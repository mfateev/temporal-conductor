package io.temporal.conductor.poc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.core.utils.ParametersUtils;
import java.util.Map;

/**
 * Minimal implementation of ParametersUtils for POC testing. For now, simply passes through
 * parameters without substitution.
 */
public class MinimalParametersUtils extends ParametersUtils {

  public MinimalParametersUtils(ObjectMapper objectMapper) {
    super(objectMapper);
  }

  @Override
  public Map<String, Object> replace(Map<String, Object> input, Object json) {
    // For POC, just return input unchanged (no parameter substitution)
    return input;
  }
}
