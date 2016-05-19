package com.datamountaineer.streamreactor.connect.jdbc.sink;

import org.junit.*;

import java.net.*;
import java.nio.file.*;
import java.util.*;

import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.DATABASE_CONNECTION_URI;
import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.EXPORT_MAPPINGS;
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

    String tableName1 = "batched_upsert_test_1";
    String tableName2 = "batched_upsert_test_2";
    String topic1 = "topic1";
    String topic2 = "topic2";
    String selected = String.format("{%s:%s;*},{%s:%s;*}", topic1, tableName1, topic2, tableName2);

    Map<String, String> props = new HashMap<>();
    props.put(DATABASE_CONNECTION_URI, SQL_LITE_URI);
    props.put(EXPORT_MAPPINGS, selected);

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
