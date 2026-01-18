package io.temporal.conductor.poc;

import com.netflix.conductor.common.run.ExternalStorageLocation;
import com.netflix.conductor.common.utils.ExternalPayloadStorage;
import java.io.ByteArrayInputStream;
import java.io.InputStream;

/** No-op implementation of ExternalPayloadStorage for POC testing. */
public class NoOpExternalPayloadStorage implements ExternalPayloadStorage {

  @Override
  public ExternalStorageLocation getLocation(Operation operation, PayloadType payloadType, String path) {
    // Return a dummy location
    ExternalStorageLocation location = new ExternalStorageLocation();
    location.setPath(path != null ? path : "noop");
    location.setUri("noop://storage");
    return location;
  }

  @Override
  public void upload(String path, InputStream payload, long payloadSize) {
    // No-op: don't store anything
  }

  @Override
  public InputStream download(String path) {
    // Return empty stream
    return new ByteArrayInputStream(new byte[0]);
  }
}
