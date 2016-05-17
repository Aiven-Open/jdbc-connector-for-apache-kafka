package com.datamountaineer.streamreactor.connect.jdbc.sink.config;

import com.google.common.collect.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.config.*;
import org.apache.kafka.connect.sink.*;
import org.junit.*;
import org.mockito.*;

import java.net.*;
import java.nio.file.*;
import java.util.*;


import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.DATABASE_CONNECTION_URI;
import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.DRIVER_MANAGER_CLASS;
import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.EXPORT_MAPPINGS;
import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.JAR_FILE;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

/**
 * Created by andrew@datamountaineer.com on 15/05/16.
 * kafka-connect-jdbc
 */
public class JdbcSettingsTest {

  @Test
  public void JdbcSettingsTestExportMap() {

    String selected = "{topic1:table1;f1->col1,f2->col5},{topic2:table2;f3->col6,f4->,f5->col7}";
    String driver = null;
    try {
      driver = Paths.get(getClass().getResource("/ojdbc7.jar").toURI()).toAbsolutePath().toString();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }

    TopicPartition tp1 = new TopicPartition("topic1", 12);
    TopicPartition tp2 = new TopicPartition("topic1", 13);
    HashSet<TopicPartition> assignment = Sets.newHashSet();

    //Set topic assignments, used by the sinkContext mock
    assignment.add(tp1);
    assignment.add(tp2);

    SinkTaskContext context = Mockito.mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(assignment);

    Map<String, String> props = new HashMap<>();
    props.put(DATABASE_CONNECTION_URI, "jdbc://");
    props.put(JAR_FILE, driver);
    props.put(DRIVER_MANAGER_CLASS, "org.sqlite.JDBC");
    props.put(EXPORT_MAPPINGS, selected);

    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings settings = JdbcSinkSettings.from(config, context);

    List<FieldsMappings> mappings = settings.getMappings();
    assertTrue(mappings.size() == 2);
    assertTrue(mappings.get(0).getTableName().equals("table1"));
    assertTrue(mappings.get(1).getTableName().equals("table2"));
    assertTrue(mappings.get(0).getIncomingTopic().equals("topic1"));
    assertTrue(mappings.get(1).getIncomingTopic().equals("topic2"));

    Map<String, FieldAlias> cols = mappings.get(0).getMappings();
    assertTrue(cols.get("f1").getName().equals("col1"));
    assertTrue(cols.get("f2").getName().equals("col5"));

    cols = mappings.get(1).getMappings();
    assertTrue(cols.get("f3").getName().equals("col6"));
    assertTrue(cols.get("f4").getName().equals("f4"));
    assertTrue(cols.get("f5").getName().equals("col7"));

    String all = "{topic1:table1;f1->col1,f2->col5},{topic2:table2;*}";
    props.clear();
    props.put(DATABASE_CONNECTION_URI, "jdbc://");
    props.put(JAR_FILE, driver);
    props.put(DRIVER_MANAGER_CLASS, "org.sqlite.JDBC");
    props.put(EXPORT_MAPPINGS, all);

    config = new JdbcSinkConfig(props);
    settings = JdbcSinkSettings.from(config, context);

    mappings = settings.getMappings();
    assertTrue(mappings.size() == 2);
    assertTrue(mappings.get(0).getTableName().equals("table1"));
    assertTrue(mappings.get(1).getTableName().equals("table2"));
    assertTrue(mappings.get(0).getIncomingTopic().equals("topic1"));
    assertTrue(mappings.get(1).getIncomingTopic().equals("topic2"));

    cols = mappings.get(0).getMappings();
    assertTrue(cols.get("f1").getName().equals("col1"));
    assertTrue(cols.get("f2").getName().equals("col5"));

    assertTrue(mappings.get(1).areAllFieldsIncluded());
    cols = mappings.get(1).getMappings();
    assertTrue(cols.isEmpty());
  }

  @Test(expected = ConfigException.class)
  public void throwTheExceptionMissingTable() {

    String driver = null;
    try {
      driver = Paths.get(getClass().getResource("/ojdbc7.jar").toURI()).toAbsolutePath().toString();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }


    TopicPartition tp1 = new TopicPartition("topic1", 12);
    TopicPartition tp2 = new TopicPartition("topic1", 13);
    HashSet<TopicPartition> assignment = Sets.newHashSet();

    //Set topic assignments, used by the sinkContext mock
    assignment.add(tp1);
    assignment.add(tp2);

    SinkTaskContext context = Mockito.mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(assignment);

    Map<String, String> props = new HashMap<>();
    //missing target
    String bad = "{topic1:;*}";
    props.put(DATABASE_CONNECTION_URI, "jdbc://");
    props.put(JAR_FILE, driver);
    props.put(DRIVER_MANAGER_CLASS, "org.sqlite.JDBC");
    props.put(EXPORT_MAPPINGS, bad);

    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings.from(config, context);
  }

  @Test(expected = ConfigException.class)
  public void throwTheExceptionMissingFields() {

    String driver = null;
    try {
      driver = Paths.get(getClass().getResource("/ojdbc7.jar").toURI()).toAbsolutePath().toString();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }


    TopicPartition tp1 = new TopicPartition("topic1", 12);
    TopicPartition tp2 = new TopicPartition("topic1", 13);
    HashSet<TopicPartition> assignment = Sets.newHashSet();

    //Set topic assignments, used by the sinkContext mock
    assignment.add(tp1);
    assignment.add(tp2);

    SinkTaskContext context = Mockito.mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(assignment);

    Map<String, String> props = new HashMap<>();
    //missing target
    String bad = "{topic1:table1;}";
    props.put(DATABASE_CONNECTION_URI, "jdbc://");
    props.put(JAR_FILE, driver);
    props.put(DRIVER_MANAGER_CLASS, "org.sqlite.JDBC");
    props.put(EXPORT_MAPPINGS, bad);

    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings.from(config, context);
  }
}
