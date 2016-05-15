package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.*;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.sql.Connection;
import java.util.Collections;

import static org.mockito.Mockito.mock;

public class NoopErrorHandlingPolicyTest {
  @Test
  public void hideTheException() {
    int retries = 0;
    new NoopErrorHandlingPolicy()
            .handle(Collections.<SinkRecord>emptyList(), new IllegalArgumentException(), mock(Connection.class), retries);
  }
}


