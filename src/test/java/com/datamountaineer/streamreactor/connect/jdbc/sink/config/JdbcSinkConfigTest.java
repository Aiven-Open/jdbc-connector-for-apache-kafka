package com.datamountaineer.streamreactor.connect.jdbc.sink.config;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.DATABASE_CONNECTION_URI;
import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.ERROR_POLICY;
import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.INSERT_MODE;
import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.TOPIC_TABLE_MAPPING;
import static org.junit.Assert.assertEquals;

public class JdbcSinkConfigTest {
  @Test
  public void shouldDefaultTheErrorPolicyToThrow() {
    Map<String, String> props = new HashMap<>();

    props.put(DATABASE_CONNECTION_URI, "jdbc://");
    props.put(TOPIC_TABLE_MAPPING, "topic1=tableA, topic2=tableB");

    assertEquals(new JdbcSinkConfig(props).getString(ERROR_POLICY), "THROW");
  }


  @Test
  public void shouldDefaultToINSERT() {
    Map<String, String> props = new HashMap<>();

    props.put(DATABASE_CONNECTION_URI, "jdbc://");
    props.put(TOPIC_TABLE_MAPPING, "topic1=tableA, topic2=tableB");

    assertEquals(new JdbcSinkConfig(props).getString(INSERT_MODE), "INSERT");
  }
}
