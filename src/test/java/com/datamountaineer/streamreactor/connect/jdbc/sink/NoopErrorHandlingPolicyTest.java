package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.*;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.Collections;

public class NoopErrorHandlingPolicyTest {
  @Test
  public void hideTheException() {
    int retries = 0;
    new NoopErrorHandlingPolicy()
            .handle(Collections.<SinkRecord>emptyList(), new IllegalArgumentException(), retries);
  }
}


