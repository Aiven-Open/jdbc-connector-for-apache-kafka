package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.*;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.sql.Connection;
import java.util.Collections;

import static org.mockito.Mockito.mock;

public class ThrowErrorHandlingPolicyTest {
  @Test(expected = RuntimeException.class)
  public void throwTheException() {
    int retries = 0;
    new ThrowErrorHandlingPolicy()
            .handle(Collections.<SinkRecord>emptyList(), new NumberFormatException(), mock(Connection.class), retries);
  }
}
