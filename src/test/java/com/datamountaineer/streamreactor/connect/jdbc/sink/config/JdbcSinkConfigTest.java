

package com.datamountaineer.streamreactor.connect.jdbc.sink.config;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.*;
import static org.junit.Assert.assertEquals;

public class JdbcSinkConfigTest {
  @Test
  public void shouldDefaultTheErrorPolicyToThrow() {
    Map<String, String> props = new HashMap<>();

    props.put(DATABASE_CONNECTION, "jdbc://");
            props.put(DATABASE_TABLE, "tablea");
            props.put(JAR_FILE, "jdbc.jar");
            props.put(DRIVER_MANAGER_CLASS, "OracleDriver");
            props.put(FIELDS, "*");
            props.put(DATABASE_IS_BATCHING, "true");

    assertEquals(new JdbcSinkConfig(props).getString(ERROR_POLICY), "throw");
  }

  @Test
  public void shouldDefaultBatchingToTrue() {
    Map<String, String> props = new HashMap<>();

    props.put(DATABASE_CONNECTION, "jdbc://");
    props.put(DATABASE_TABLE, "tablea");
    props.put(JAR_FILE, "jdbc.jar");
    props.put(DRIVER_MANAGER_CLASS, "OracleDriver");
    props.put(FIELDS, "*");

    assertEquals(new JdbcSinkConfig(props).getBoolean(DATABASE_IS_BATCHING), true);
  }

  @Test
  public void handleFieldsNotBeingSpecified() {
    Map<String, String> props = new HashMap<>();

    props.put(DATABASE_CONNECTION, "jdbc://");
    props.put(DATABASE_TABLE, "tablea");
    props.put(JAR_FILE, "jdbc.jar");
    props.put(DRIVER_MANAGER_CLASS, "OracleDriver");

    assertEquals(new JdbcSinkConfig(props).getString(FIELDS), "*");
  }
}
