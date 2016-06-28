package io.confluent.connect.jdbc.sink;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.sink.config.JdbcSinkConfig;

import static junit.framework.TestCase.assertTrue;

public class SinkConnectorTest {

  private static final String DB_FILE = "test_db_writer_sqllite.db";
  private static final String SQL_LITE_URI = "jdbc:sqlite:" + DB_FILE;

  @Test
  public void SinkConnectorTestStart() {

    TestBase base = new TestBase();
    Map<String, String> props = base.getPropsAllFields("throw", "insert", false);
    props.put(JdbcSinkConfig.DATABASE_CONNECTION_URI, SQL_LITE_URI);

    JdbcSinkConnector connector = new JdbcSinkConnector();
    connector.start(props);
    assertTrue(connector.taskClass().getCanonicalName().equals(JdbcSinkTask.class.getCanonicalName()));
    List<Map<String, String>> config = connector.taskConfigs(1);
    assertTrue(config.size() == 1);

    //get a random field
    assertTrue(config.get(0).containsKey(JdbcSinkConfig.DATABASE_CONNECTION_URI));
    connector.start(props);
    connector.stop();
  }
}
