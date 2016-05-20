package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.*;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.Collections;

public class ThrowErrorHandlingPolicyTest {
  @Test(expected = RuntimeException.class)
  public void throwTheException() {
    int retries = 0;
    new ThrowErrorHandlingPolicy()
            .handle(Collections.<SinkRecord>emptyList(), new NumberFormatException(), retries);
  }
}
