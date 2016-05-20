package com.datamountaineer.streamreactor.connect.jdbc.sink;

import org.junit.*;

import java.util.*;

import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.DATABASE_CONNECTION_URI;
import static junit.framework.TestCase.assertTrue;

/**
 * Created by andrew@datamountaineer.com on 15/05/16.
 * kafka-connect-jdbc
 */
public class SinkConnectorTest {

  private static final String DB_FILE = "test_db_writer_sqllite.db";
  private static final String SQL_LITE_URI = "jdbc:sqlite:" + DB_FILE;

  @Test
  public void SinkConnectorTestStart() {

    TestBase base = new TestBase();
    Map<String, String> props = base.getPropsAllFields("throw", "insert", false);
    props.put(DATABASE_CONNECTION_URI, SQL_LITE_URI);

    JdbcSinkConnector connector = new JdbcSinkConnector();
    connector.start(props);
    assertTrue(connector.taskClass().getCanonicalName().equals(JdbcSinkTask.class.getCanonicalName()));
    List<Map<String, String>> config = connector.taskConfigs(1);
    assertTrue(config.size() == 1);

    //get a random field
    assertTrue(config.get(0).containsKey(DATABASE_CONNECTION_URI));
    connector.start(props);
    connector.stop();
  }
}
