package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.*;
import org.apache.kafka.connect.errors.*;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.sql.Connection;
import java.util.Collections;

import static org.mockito.Mockito.mock;

public class RetryErrorHandlingPolicyTest {
  @Test(expected = RetriableException.class)
  public void throwRetry() {
    new RetryErrorHandlingPolicy()
        .handle(Collections.<SinkRecord>emptyList(), new NumberFormatException(), mock(Connection.class), 10);
  }

  @Test(expected = RuntimeException.class)
  public void throwTheException() {
    new RetryErrorHandlingPolicy()
        .handle(Collections.<SinkRecord>emptyList(), new NumberFormatException(), mock(Connection.class), 0);
  }
}