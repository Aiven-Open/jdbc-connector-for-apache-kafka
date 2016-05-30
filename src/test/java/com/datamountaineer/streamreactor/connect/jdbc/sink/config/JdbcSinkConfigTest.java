package com.datamountaineer.streamreactor.connect.jdbc.sink.config;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.DATABASE_CONNECTION_URI;
import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.ERROR_POLICY;
import static org.junit.Assert.assertEquals;

public class JdbcSinkConfigTest {
  @Test
  public void shouldDefaultTheErrorPolicyToThrow() {
    Map<String, String> props = new HashMap<>();

    props.put(DATABASE_CONNECTION_URI, "jdbc://");
    props.put(JdbcSinkConfig.EXPORT_MAPPINGS,
            "INSERT INTO tableA SELECT * FROM topic1;INSERT INTO tableB SELECT * FROM topic2");

    JdbcSinkConfig config = new JdbcSinkConfig(props);
    assertEquals(config.getString(ERROR_POLICY), "THROW");

  }
}
