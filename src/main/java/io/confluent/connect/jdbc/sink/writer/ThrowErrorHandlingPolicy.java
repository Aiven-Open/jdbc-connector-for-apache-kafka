package io.confluent.connect.jdbc.sink.writer;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;

/**
 * The policy propagates the error further
 */
public final class ThrowErrorHandlingPolicy implements ErrorHandlingPolicy {
  @Override
  public void handle(Collection<SinkRecord> records, final Throwable error, final int retryCount) {
    throw new RuntimeException(error);
  }

}
