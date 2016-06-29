package io.confluent.connect.jdbc.sink;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.confluent.connect.jdbc.sink.config.JdbcSinkConfig;

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
  private static final String insert1 =
      "INSERT INTO " + tableName1 + " SELECT f1 as col1, f2 FROM " + topic1;

  private static final String insertAll1 =
      "INSERT INTO " + tableName1 + " SELECT * FROM " + topic1;

  private static final String upsert1 =
      "UPSERT INTO " + tableName1 + " SELECT f1 as col1, f2 FROM " + topic1;

  private static final String upsertAll1 =
      "UPSERT INTO " + tableName1 + " SELECT * FROM " + topic1;


  private static final String insert2 =
      "INSERT INTO " + tableName2 + " SELECT f3 as col3, f4 as col4 FROM " + topic2;

  private static final String insertAll2 =
      "INSERT INTO " + tableName2 + " SELECT * FROM " + topic2;

  private static final String upsert2 =
      "UPSERT INTO " + tableName2 + " SELECT f3 as col3, f4 as col4 FROM " + topic2;

  private static final String upsertAll2 =
      "UPSERT INTO " + tableName2 + " SELECT * FROM " + topic2;

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

  public Map<String, String> getPropsAllFields(String errorPolicy, String mode, Boolean autoCreate) {
    Map<String, String> props = new HashMap<>();

    props.put("topic", topics);
    props.put(JdbcSinkConfig.DATABASE_CONNECTION_URI, "jdbc://");
    props.put(JdbcSinkConfig.DATABASE_CONNECTION_USER, "");
    props.put(JdbcSinkConfig.DATABASE_CONNECTION_PASSWORD, "");
    props.put(JdbcSinkConfig.ERROR_POLICY, errorPolicy);
    if (autoCreate) {
      if (Objects.equals(mode.toLowerCase(), "upsert")) {
        props.put(JdbcSinkConfig.EXPORT_MAPPINGS, upsertAll1 + " AUTOCREATE;" + upsertAll2);
      } else {
        props.put(JdbcSinkConfig.EXPORT_MAPPINGS, insertAll1 + " AUTOCREATE;" + insert2);
      }
    } else {
      if (Objects.equals(mode.toLowerCase(), "upsert")) {
        props.put(JdbcSinkConfig.EXPORT_MAPPINGS, upsertAll1 + ";" + upsertAll2);
      } else {
        props.put(JdbcSinkConfig.EXPORT_MAPPINGS, insertAll1 + ";" + insertAll2);
      }
    }
    return props;
  }

  public Map<String, String> getPropsAllFieldsAutoCreatePK() {
    Map<String, String> map = getPropsAllFields("throw", "upsert", true);
    map.put(JdbcSinkConfig.EXPORT_MAPPINGS, upsertAll1 + " AUTOCREATE PK f1,f2;" + upsertAll2 + " AUTOCREATE PK f3");
    return map;
  }

  public Map<String, String> getPropsSelectedFieldsAutoCreatePKBad() {
    Map<String, String> map = getPropsSelectedFields("throw", "upsert", true);
    map.put(JdbcSinkConfig.EXPORT_MAPPINGS, insert1 + " AUTOCREATE;" + insert2 + " AUTOCREATE PK f13");
    return map;
  }

  public Map<String, String> getPropsSelectedFields(String errorPolicy, String mode, Boolean autoCreate) {
    Map<String, String> props = new HashMap<>();

    props.put("topic", topics);
    props.put(JdbcSinkConfig.DATABASE_CONNECTION_URI, "jdbc://");
    props.put(JdbcSinkConfig.DATABASE_CONNECTION_USER, "");
    props.put(JdbcSinkConfig.DATABASE_CONNECTION_PASSWORD, "");
    props.put(JdbcSinkConfig.ERROR_POLICY, errorPolicy);

    if (autoCreate) {
      if (Objects.equals(mode, "upsert")) {
        props.put(JdbcSinkConfig.EXPORT_MAPPINGS, upsert1 + " AUTOCREATE;" + upsert2);
      } else {
        props.put(JdbcSinkConfig.EXPORT_MAPPINGS, insert1 + " AUTOCREATE;" + insert2);
      }
    } else {
      if (Objects.equals(mode, "upsert")) {
        props.put(JdbcSinkConfig.EXPORT_MAPPINGS, upsert1 + ";" + upsert2);
      } else {
        props.put(JdbcSinkConfig.EXPORT_MAPPINGS, insert1 + ";" + insert2);
      }
    }
    return props;
  }

}
