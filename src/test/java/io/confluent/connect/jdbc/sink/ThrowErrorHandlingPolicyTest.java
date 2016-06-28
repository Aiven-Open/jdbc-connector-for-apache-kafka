package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.Collections;

import io.confluent.connect.jdbc.sink.writer.ThrowErrorHandlingPolicy;

public class ThrowErrorHandlingPolicyTest {
  @Test(expected = RuntimeException.class)
  public void throwTheException() {
    int retries = 0;
    new ThrowErrorHandlingPolicy()
            .handle(Collections.<SinkRecord>emptyList(), new NumberFormatException(), retries);
  }
}
