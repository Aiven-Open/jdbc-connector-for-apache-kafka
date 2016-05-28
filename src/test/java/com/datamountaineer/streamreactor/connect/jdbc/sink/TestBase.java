package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldAlias;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldsMappings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.AUTO_CREATE_TABLE_MAP;
import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.DATABASE_CONNECTION_PASSWORD;
import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.DATABASE_CONNECTION_URI;
import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.DATABASE_CONNECTION_USER;
import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.ERROR_POLICY;
import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.EXPORT_MAPPINGS;
import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.INSERT_MODE;

/**
 * Created by andrew@datamountaineer.com on 20/05/16.
 * kafka-connect-jdbc
 */


//name=jdbc-datamountaineer-1
//    connector.class=com.datamountaineer.streamreactor.connect.jdbc.sink.JdbcSinkConnector
//    tasks.max=1
//    topics=orders
//    connect.jdbc.connection.uri=jdbc:sqlite:test.db
//    connect.jdbc.connection.user=
//    connect.jdbc.connection.password=
//    connect.jdbc.sink.error.policy=RETRY
//    connect.jdbc.sink.batching.enabled=true
//    connect.jdbc.sink.export.mappings={orders:orders;qty->quantity,product->,price->}
//    connect.jdbc.sink.mode=INSERT

public class TestBase {

  private static final String tableName1 = "tableA";
  private static final String tableName2 = "tableB";
  private static final String topic1 = "topic1";
  private static final String topic2 = "topic2";
  private static final String topics = topic1 + "," + topic2;
  private static final String selection = "{" + topic1 + ":" + tableName1 + ";f1->col1,f2->},{" + topic2 + ":" +
          tableName2 + ";f3->col3,f4->col4}";
  private static final String all = "{" + topic1 + ":" + tableName1 + ";*},{" + topic2 + ":" + tableName2 + ";*}";

  public String getAllMap() {
    return all;
  }

  public String getSelectionMap() {
    return selection;
  }

  public String getTableName1() {
    return tableName1;
  }

  public String getTableName2() {
    return tableName2;
  }

  public String getTopic1() {
    return topic1;
  }

  public String getTopic2() {
    return topic2;
  }

  public List<FieldsMappings> getFieldMappingsSelection() {
//    final String tableName,
//    final String incomingTopic,
//    final boolean allFieldsIncluded,
//    final Map<String, FieldAlias> mappings
    List<FieldsMappings> list = Lists.newArrayList();

    Map<String, FieldAlias> mappings = Maps.newHashMap();
    mappings.put("f1", new FieldAlias("col1", false));
    mappings.put("f2", new FieldAlias("col2", false));
    list.add(new FieldsMappings(tableName1, topic1, false, mappings));
    Map<String, FieldAlias> mappings2 = Maps.newHashMap();
    mappings2.put("f3", new FieldAlias("col3", false));
    mappings2.put("f4", new FieldAlias("col4", false));
    list.add(new FieldsMappings(tableName2, topic2, false, mappings2));
    return list;
  }

  public List<FieldsMappings> getFieldMappingsAll() {
    List<FieldsMappings> list = Lists.newArrayList();

    Map<String, FieldAlias> mappings = Maps.newHashMap();
    list.add(new FieldsMappings(tableName1, topic1, true, mappings, false, false));
    Map<String, FieldAlias> mappings2 = Maps.newHashMap();
    list.add(new FieldsMappings(tableName2, topic2, true, mappings2, false, false));
    return list;
  }

  public Map<String, String> getPropsAllFields(String errorPolicy, String mode, Boolean autoCreate) {

    Map<String, String> props = new HashMap<>();

    props.put("topic", topics);
    props.put(DATABASE_CONNECTION_URI, "jdbc://");
    props.put(DATABASE_CONNECTION_USER, "");
    props.put(DATABASE_CONNECTION_PASSWORD, "");
    props.put(ERROR_POLICY, errorPolicy);
    props.put(INSERT_MODE, mode);
    props.put(EXPORT_MAPPINGS, all);

    if (autoCreate) {
      //only topic 1
      props.put(AUTO_CREATE_TABLE_MAP, "{" + topic1 + ":}");
    }
    return props;
  }

  public Map<String, String> getPropsAllFieldsAutoCreatePK() {
    Map<String, String> map = getPropsAllFields("throw", "upsert", true);
    map.put(AUTO_CREATE_TABLE_MAP, "{" + topic1 + ":f1,f2}" + ", " + "{" + topic2 + ":f3}");
    return map;
  }

  public Map<String, String> getPropsSelectedFieldsAutoCreatePKBad() {
    Map<String, String> map = getPropsSelectedFields("throw", "upsert", true);
    map.put(AUTO_CREATE_TABLE_MAP, "{" + topic1 + ":}" + ", " + "{" + topic2 + ":f13}");
    return map;
  }

  public Map<String, String> getPropsSelectedFields(String errorPolicy, String mode, Boolean autoCreate) {
    Map<String, String> props = new HashMap<>();

    props.put("topic", topics);
    props.put(DATABASE_CONNECTION_URI, "jdbc://");
    props.put(DATABASE_CONNECTION_USER, "");
    props.put(DATABASE_CONNECTION_PASSWORD, "");
    props.put(ERROR_POLICY, errorPolicy);
    props.put(INSERT_MODE, mode);
    props.put(EXPORT_MAPPINGS, selection);

    if (autoCreate) {
      //only topic 1
      props.put(AUTO_CREATE_TABLE_MAP, "{" + topic1 + ":}");
    }

    return props;
  }

}
