package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.*;
import org.apache.kafka.connect.errors.*;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.Collections;

public class RetryErrorHandlingPolicyTest {
  @Test(expected = RetriableException.class)
  public void throwRetry() {
    new RetryErrorHandlingPolicy()
        .handle(Collections.<SinkRecord>emptyList(), new NumberFormatException(), 10);
  }

  @Test(expected = RuntimeException.class)
  public void throwTheException() {
    new RetryErrorHandlingPolicy()
        .handle(Collections.<SinkRecord>emptyList(), new NumberFormatException(), 0);
  }
}