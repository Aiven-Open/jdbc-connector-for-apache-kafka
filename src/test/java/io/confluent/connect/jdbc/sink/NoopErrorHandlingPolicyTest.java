package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.Collections;

import io.confluent.connect.jdbc.sink.writer.NoopErrorHandlingPolicy;

public class NoopErrorHandlingPolicyTest {
  @Test
  public void hideTheException() {
    int retries = 0;
    new NoopErrorHandlingPolicy()
            .handle(Collections.<SinkRecord>emptyList(), new IllegalArgumentException(), retries);
  }
}


