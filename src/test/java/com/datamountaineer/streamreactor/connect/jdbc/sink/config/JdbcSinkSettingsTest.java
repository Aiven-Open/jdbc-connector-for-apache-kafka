package com.datamountaineer.streamreactor.connect.jdbc.sink.config;


import com.datamountaineer.streamreactor.connect.jdbc.sink.TestBase;
import com.datamountaineer.streamreactor.connect.jdbc.sink.services.*;
import com.google.common.collect.Sets;
import io.confluent.common.config.ConfigException;
import io.confluent.kafka.schemaregistry.client.rest.*;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.*;
import org.apache.curator.test.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.*;
import org.mockito.Mockito;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.AUTO_CREATE_TABLE_MAP;
import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.DATABASE_CONNECTION_URI;
import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.ERROR_POLICY;
import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.EXPORT_MAPPINGS;
import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.SCHEMA_REGISTRY_URL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class JdbcSinkSettingsTest {

  private static final String DB_FILE = "test_db_writer_sqllite.db";
  private static final String SQL_LITE_URI = "jdbc:sqlite:" + DB_FILE;
  int port;
  TestBase base = new TestBase();
  RestService client;


  String rawSchema = "{\"type\":\"record\",\"name\":\"myrecord\",\n" +
      "\"fields\":[\n" +
      "{\"name\":\"firstName\",\"type\":[\"null\", \"string\"]},\n" +
      "{\"name\":\"lastName\", \"type\": \"string\"}, \n" +
      "{\"name\":\"age\", \"type\": \"int\"}, \n" +
      "{\"name\":\"bool\", \"type\": \"float\"},\n" +
      "{\"name\":\"byte\", \"type\": \"float\"},\n" +
      "{\"name\":\"short\", \"type\": [\"null\", \"int\"]},\n" +
      "{\"name\":\"long\", \"type\": \"long\"},\n" +
      "{\"name\":\"float\", \"type\": \"float\"},\n" +
      "{\"name\":\"double\", \"type\": \"double\"}\n" +
      "]}";


  @Before
  public void setUp() throws Exception {
    deleteSqlLiteFile();
    port = InstanceSpec.getRandomPort();
    EmbeddedSingleNodeKafkaCluster cluster  = new EmbeddedSingleNodeKafkaCluster();
    RestApp registry = new RestApp(port, cluster.zookeeperConnect(), base.getTopic1());
    registry.start();
    client = registry.restClient;
  }

  @After
  public void tearDown() {
    deleteSqlLiteFile();
  }

  private void deleteSqlLiteFile() {
    new File(DB_FILE).delete();
  }

  @Test
  public void InsertThrowBatchingAllFields() throws Exception {
    Map<String, String> props = base.getPropsAllFields("throw", "insert", false);

    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings settings = JdbcSinkSettings.from(config);
    assertTrue(settings.getInsertMode().equals(InsertModeEnum.INSERT));
    assertTrue(settings.getErrorPolicy().equals(ErrorPolicyEnum.THROW));
    assertTrue(settings.isBatching());

    List<FieldsMappings> mappings = settings.getMappings();
    assertTrue(mappings.size() == 2);
    assertTrue(mappings.get(0).getTableName().equals(base.getTableName1()));
    assertTrue(mappings.get(1).getTableName().equals(base.getTableName2()));
    assertTrue(mappings.get(0).getIncomingTopic().equals(base.getTopic1()));
    assertTrue(mappings.get(1).getIncomingTopic().equals(base.getTopic2()));

    assertTrue(mappings.get(0).getMappings().isEmpty());
    assertTrue(mappings.get(1).getMappings().isEmpty());

    assertTrue(mappings.get(0).areAllFieldsIncluded());
    assertTrue(mappings.get(1).areAllFieldsIncluded());
    assertFalse(mappings.get(0).autoCreateTable());
    assertFalse(mappings.get(1).autoCreateTable());

    assertFalse(mappings.get(0).evolveTableSchema());
    assertFalse(mappings.get(1).evolveTableSchema());
  }


  @Test
  public void InsertThrowBatchingSelectedFields() throws IOException, RestClientException {
    TestBase base = new TestBase();
    Map<String, String> props = base.getPropsSelectedFields("throw", "insert", false);


    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings settings = JdbcSinkSettings.from(config);
    assertTrue(settings.getInsertMode().equals(InsertModeEnum.INSERT));
    assertTrue(settings.getErrorPolicy().equals(ErrorPolicyEnum.THROW));
    assertTrue(settings.isBatching());

    List<FieldsMappings> mappings = settings.getMappings();
    assertTrue(mappings.size() == 2);
    assertTrue(mappings.get(0).getTableName().equals(base.getTableName1()));
    assertTrue(mappings.get(1).getTableName().equals(base.getTableName2()));
    assertTrue(mappings.get(0).getIncomingTopic().equals(base.getTopic1()));
    assertTrue(mappings.get(1).getIncomingTopic().equals(base.getTopic2()));

    assertTrue(mappings.get(0).getMappings().size() == 2);
    assertTrue(mappings.get(1).getMappings().size() == 2);

    assertFalse(mappings.get(0).areAllFieldsIncluded());
    assertFalse(mappings.get(1).areAllFieldsIncluded());
    assertFalse(mappings.get(0).autoCreateTable());
    assertFalse(mappings.get(1).autoCreateTable());

    assertFalse(mappings.get(0).evolveTableSchema());
    assertFalse(mappings.get(1).evolveTableSchema());

    Map<String, FieldAlias> cols = mappings.get(0).getMappings();
    assertTrue(cols.get("f1").getName().equals("col1"));
    assertTrue(cols.get("f2").getName().equals("f2"));

    cols = mappings.get(1).getMappings();
    assertTrue(cols.get("f3").getName().equals("col3"));
    assertTrue(cols.get("f4").getName().equals("col4"));
  }

  @Test
  public void InsertNoopBatchingAllFields() throws IOException, RestClientException {
    TestBase base = new TestBase();
    Map<String, String> props = base.getPropsAllFields("noop", "insert", false);


    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings settings = JdbcSinkSettings.from(config);
    assertTrue(settings.getInsertMode().equals(InsertModeEnum.INSERT));
    assertTrue(settings.getErrorPolicy().equals(ErrorPolicyEnum.NOOP));
    assertTrue(settings.isBatching());

    List<FieldsMappings> mappings = settings.getMappings();
    assertTrue(mappings.size() == 2);
    assertTrue(mappings.get(0).getTableName().equals(base.getTableName1()));
    assertTrue(mappings.get(1).getTableName().equals(base.getTableName2()));
    assertTrue(mappings.get(0).getIncomingTopic().equals(base.getTopic1()));
    assertTrue(mappings.get(1).getIncomingTopic().equals(base.getTopic2()));

    assertTrue(mappings.get(0).getMappings().isEmpty());
    assertTrue(mappings.get(1).getMappings().isEmpty());

    assertTrue(mappings.get(0).areAllFieldsIncluded());
    assertTrue(mappings.get(1).areAllFieldsIncluded());
    assertFalse(mappings.get(0).autoCreateTable());
    assertFalse(mappings.get(1).autoCreateTable());

    assertFalse(mappings.get(0).evolveTableSchema());
    assertFalse(mappings.get(1).evolveTableSchema());
  }


  @Test
  public void InsertNoopBatchingSelectedFields() throws IOException, RestClientException {
    TestBase base = new TestBase();
    Map<String, String> props = base.getPropsSelectedFields("noop", "insert", false);

    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings settings = JdbcSinkSettings.from(config);
    assertTrue(settings.getInsertMode().equals(InsertModeEnum.INSERT));
    assertTrue(settings.getErrorPolicy().equals(ErrorPolicyEnum.NOOP));
    assertTrue(settings.isBatching());

    List<FieldsMappings> mappings = settings.getMappings();
    assertTrue(mappings.size() == 2);
    assertTrue(mappings.get(0).getTableName().equals(base.getTableName1()));
    assertTrue(mappings.get(1).getTableName().equals(base.getTableName2()));
    assertTrue(mappings.get(0).getIncomingTopic().equals(base.getTopic1()));
    assertTrue(mappings.get(1).getIncomingTopic().equals(base.getTopic2()));

    assertTrue(mappings.get(0).getMappings().size() == 2);
    assertTrue(mappings.get(1).getMappings().size() == 2);

    assertFalse(mappings.get(0).areAllFieldsIncluded());
    assertFalse(mappings.get(1).areAllFieldsIncluded());
    assertFalse(mappings.get(0).autoCreateTable());
    assertFalse(mappings.get(1).autoCreateTable());

    assertFalse(mappings.get(0).evolveTableSchema());
    assertFalse(mappings.get(1).evolveTableSchema());

    Map<String, FieldAlias> cols = mappings.get(0).getMappings();
    assertTrue(cols.get("f1").getName().equals("col1"));
    assertTrue(cols.get("f2").getName().equals("f2"));

    cols = mappings.get(1).getMappings();
    assertTrue(cols.get("f3").getName().equals("col3"));
    assertTrue(cols.get("f4").getName().equals("col4"));
  }


  @Test
  public void InsertRetryBatchingAllFields() throws IOException, RestClientException {
    TestBase base = new TestBase();
    Map<String, String> props = base.getPropsAllFields("retry", "insert", false);

    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings settings = JdbcSinkSettings.from(config);
    assertTrue(settings.getInsertMode().equals(InsertModeEnum.INSERT));
    assertTrue(settings.getErrorPolicy().equals(ErrorPolicyEnum.RETRY));
    assertTrue(settings.isBatching());
    assertTrue(settings.getRetries() == 10);

    List<FieldsMappings> mappings = settings.getMappings();
    assertTrue(mappings.size() == 2);
    assertTrue(mappings.get(0).getTableName().equals(base.getTableName1()));
    assertTrue(mappings.get(1).getTableName().equals(base.getTableName2()));
    assertTrue(mappings.get(0).getIncomingTopic().equals(base.getTopic1()));
    assertTrue(mappings.get(1).getIncomingTopic().equals(base.getTopic2()));

    assertTrue(mappings.get(0).getMappings().isEmpty());
    assertTrue(mappings.get(1).getMappings().isEmpty());

    assertTrue(mappings.get(0).areAllFieldsIncluded());
    assertTrue(mappings.get(1).areAllFieldsIncluded());
    assertFalse(mappings.get(0).autoCreateTable());
    assertFalse(mappings.get(1).autoCreateTable());

    assertFalse(mappings.get(0).evolveTableSchema());
    assertFalse(mappings.get(1).evolveTableSchema());
  }


  @Test
  public void InsertRetryBatchingSelectedFields() throws IOException, RestClientException {
    TestBase base = new TestBase();
    Map<String, String> props = base.getPropsSelectedFields("retry", "insert", false);

    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings settings = JdbcSinkSettings.from(config);
    assertTrue(settings.getInsertMode().equals(InsertModeEnum.INSERT));
    assertTrue(settings.getErrorPolicy().equals(ErrorPolicyEnum.RETRY));
    assertTrue(settings.isBatching());
    assertTrue(settings.getRetries() == 10);

    List<FieldsMappings> mappings = settings.getMappings();
    assertTrue(mappings.size() == 2);
    assertTrue(mappings.get(0).getTableName().equals(base.getTableName1()));
    assertTrue(mappings.get(1).getTableName().equals(base.getTableName2()));
    assertTrue(mappings.get(0).getIncomingTopic().equals(base.getTopic1()));
    assertTrue(mappings.get(1).getIncomingTopic().equals(base.getTopic2()));

    assertTrue(mappings.get(0).getMappings().size() == 2);
    assertTrue(mappings.get(1).getMappings().size() == 2);

    assertFalse(mappings.get(0).areAllFieldsIncluded());
    assertFalse(mappings.get(1).areAllFieldsIncluded());
    assertFalse(mappings.get(0).autoCreateTable());
    assertFalse(mappings.get(1).autoCreateTable());

    assertFalse(mappings.get(0).evolveTableSchema());
    assertFalse(mappings.get(1).evolveTableSchema());

    Map<String, FieldAlias> cols = mappings.get(0).getMappings();
    assertTrue(cols.get("f1").getName().equals("col1"));
    assertTrue(cols.get("f2").getName().equals("f2"));

    cols = mappings.get(1).getMappings();
    assertTrue(cols.get("f3").getName().equals("col3"));
    assertTrue(cols.get("f4").getName().equals("col4"));
  }


  @Test
  public void UpsertThrowBatchingAllFields() throws IOException, RestClientException {
    TestBase base = new TestBase();
    Map<String, String> props = base.getPropsAllFields("throw", "upsert", false);

    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings settings = JdbcSinkSettings.from(config);
    assertTrue(settings.getInsertMode().equals(InsertModeEnum.UPSERT));
    assertTrue(settings.getErrorPolicy().equals(ErrorPolicyEnum.THROW));
    assertTrue(settings.isBatching());


    List<FieldsMappings> mappings = settings.getMappings();
    assertTrue(mappings.size() == 2);
    assertTrue(mappings.get(0).getTableName().equals(base.getTableName1()));
    assertTrue(mappings.get(1).getTableName().equals(base.getTableName2()));
    assertTrue(mappings.get(0).getIncomingTopic().equals(base.getTopic1()));
    assertTrue(mappings.get(1).getIncomingTopic().equals(base.getTopic2()));

    assertTrue(mappings.get(0).getMappings().isEmpty());
    assertTrue(mappings.get(1).getMappings().isEmpty());

    assertTrue(mappings.get(0).areAllFieldsIncluded());
    assertTrue(mappings.get(1).areAllFieldsIncluded());
    assertFalse(mappings.get(0).autoCreateTable());
    assertFalse(mappings.get(1).autoCreateTable());

    assertFalse(mappings.get(0).evolveTableSchema());
    assertFalse(mappings.get(1).evolveTableSchema());
  }


  @Test
  public void UpsertThrowBatchingSelectedFields() throws IOException, RestClientException {
    TestBase base = new TestBase();
    Map<String, String> props = base.getPropsSelectedFields("throw", "upsert", false);

    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings settings = JdbcSinkSettings.from(config);
    assertTrue(settings.getInsertMode().equals(InsertModeEnum.UPSERT));
    assertTrue(settings.getErrorPolicy().equals(ErrorPolicyEnum.THROW));
    assertTrue(settings.isBatching());

    List<FieldsMappings> mappings = settings.getMappings();
    assertTrue(mappings.size() == 2);
    assertTrue(mappings.get(0).getTableName().equals(base.getTableName1()));
    assertTrue(mappings.get(1).getTableName().equals(base.getTableName2()));
    assertTrue(mappings.get(0).getIncomingTopic().equals(base.getTopic1()));
    assertTrue(mappings.get(1).getIncomingTopic().equals(base.getTopic2()));

    assertTrue(mappings.get(0).getMappings().size() == 2);
    assertTrue(mappings.get(1).getMappings().size() == 2);

    assertFalse(mappings.get(0).areAllFieldsIncluded());
    assertFalse(mappings.get(1).areAllFieldsIncluded());
    assertFalse(mappings.get(0).autoCreateTable());
    assertFalse(mappings.get(1).autoCreateTable());

    assertFalse(mappings.get(0).evolveTableSchema());
    assertFalse(mappings.get(1).evolveTableSchema());

    Map<String, FieldAlias> cols = mappings.get(0).getMappings();
    assertTrue(cols.get("f1").getName().equals("col1"));
    assertTrue(cols.get("f2").getName().equals("f2"));

    cols = mappings.get(1).getMappings();
    assertTrue(cols.get("f3").getName().equals("col3"));
    assertTrue(cols.get("f4").getName().equals("col4"));
  }

  @Test
  public void UpsertThrowBatchingSelectedFieldsAutoCreateDefault() throws IOException, RestClientException {
    TestBase base = new TestBase();
    Map<String, String> props = base.getPropsSelectedFields("throw", "upsert", true);

    client.registerSchema(rawSchema, base.getTopic1());
    props.put(DATABASE_CONNECTION_URI, SQL_LITE_URI);
    props.put(AUTO_CREATE_TABLE_MAP, "{" + base.getTopic1() + ":}");
    props.put(SCHEMA_REGISTRY_URL, "http://localhost:" + port);

    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings settings = JdbcSinkSettings.from(config);
    assertTrue(settings.getInsertMode().equals(InsertModeEnum.UPSERT));
    assertTrue(settings.getErrorPolicy().equals(ErrorPolicyEnum.THROW));
    assertTrue(settings.isBatching());

    List<FieldsMappings> mappings = settings.getMappings();
    assertTrue(mappings.size() == 2);
    assertTrue(mappings.get(0).getTableName().equals(base.getTableName1()));
    assertTrue(mappings.get(1).getTableName().equals(base.getTableName2()));
    assertTrue(mappings.get(0).getIncomingTopic().equals(base.getTopic1()));
    assertTrue(mappings.get(1).getIncomingTopic().equals(base.getTopic2()));

    assertTrue(mappings.get(0).getMappings().size() == 2);
    assertTrue(mappings.get(1).getMappings().size() == 2);

    assertFalse(mappings.get(0).areAllFieldsIncluded());
    assertFalse(mappings.get(1).areAllFieldsIncluded());
    assertTrue(mappings.get(0).autoCreateTable()); //only table1
    assertFalse(mappings.get(1).autoCreateTable());

    assertFalse(mappings.get(0).evolveTableSchema());
    assertFalse(mappings.get(1).evolveTableSchema());

    Map<String, FieldAlias> cols = mappings.get(0).getMappings();
    assertTrue(cols.get("f1").getName().equals("col1"));
    assertTrue(cols.get("f2").getName().equals("f2"));

    cols = mappings.get(1).getMappings();
    assertTrue(cols.get("f3").getName().equals("col3"));
    assertTrue(cols.get("f4").getName().equals("col4"));
  }

  @Test
  public void UpsertThrowBatchingAllFieldsAutoCreateDefault() throws IOException, RestClientException {
    TestBase base = new TestBase();
    Map<String, String> props = base.getPropsAllFields("throw", "upsert", true);

    client.registerSchema(rawSchema, base.getTopic1());
    props.put(DATABASE_CONNECTION_URI, SQL_LITE_URI);
    props.put(AUTO_CREATE_TABLE_MAP, "{" + base.getTopic1() + ":}");
    props.put(SCHEMA_REGISTRY_URL, "http://localhost:" + port);

    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings settings = JdbcSinkSettings.from(config);
    assertTrue(settings.getInsertMode().equals(InsertModeEnum.UPSERT));
    assertTrue(settings.getErrorPolicy().equals(ErrorPolicyEnum.THROW));
    assertTrue(settings.isBatching());

    List<FieldsMappings> mappings = settings.getMappings();
    assertTrue(mappings.size() == 2);
    assertTrue(mappings.get(0).getTableName().equals(base.getTableName1()));
    assertTrue(mappings.get(1).getTableName().equals(base.getTableName2()));
    assertTrue(mappings.get(0).getIncomingTopic().equals(base.getTopic1()));
    assertTrue(mappings.get(1).getIncomingTopic().equals(base.getTopic2()));

    assertTrue(mappings.get(0).areAllFieldsIncluded());
    assertTrue(mappings.get(1).areAllFieldsIncluded());
    assertTrue(mappings.get(0).autoCreateTable()); //only table1
    assertFalse(mappings.get(1).autoCreateTable());

    assertFalse(mappings.get(0).evolveTableSchema());
    assertFalse(mappings.get(1).evolveTableSchema());

    assertEquals(1, mappings.get(0).getMappings().size());
    assertTrue(mappings.get(1).getMappings().isEmpty());
  }

  @Test
  public void UpsertThrowBatchingAllFieldsAutoCreate() throws IOException, RestClientException {
    TestBase base = new TestBase();
    Map<String, String> props = base.getPropsAllFieldsAutoCreatePK();


    client.registerSchema(rawSchema, base.getTopic1());
    props.put(DATABASE_CONNECTION_URI, SQL_LITE_URI);
//    props.put(AUTO_CREATE_TABLE_MAP, "{" + base.getTopic1() + ":}");
    props.put(SCHEMA_REGISTRY_URL, "http://localhost:" + port);


    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings settings = JdbcSinkSettings.from(config);
    assertTrue(settings.getInsertMode().equals(InsertModeEnum.UPSERT));
    assertTrue(settings.getErrorPolicy().equals(ErrorPolicyEnum.THROW));
    assertTrue(settings.isBatching());

    List<FieldsMappings> mappings = settings.getMappings();
    assertTrue(mappings.size() == 2);
    assertTrue(mappings.get(0).getTableName().equals(base.getTableName1()));
    assertTrue(mappings.get(1).getTableName().equals(base.getTableName2()));
    assertTrue(mappings.get(0).getIncomingTopic().equals(base.getTopic1()));
    assertTrue(mappings.get(1).getIncomingTopic().equals(base.getTopic2()));

    assertTrue(mappings.get(0).areAllFieldsIncluded());
    assertTrue(mappings.get(1).areAllFieldsIncluded());
    assertTrue(mappings.get(0).autoCreateTable()); //only table1
    assertTrue(mappings.get(1).autoCreateTable());

    assertFalse(mappings.get(0).evolveTableSchema());
    assertFalse(mappings.get(1).evolveTableSchema());

    assertTrue(mappings.get(0).getMappings().size() == 1); //auto PK
    assertTrue(mappings.get(0).getMappings().get(FieldsMappings.CONNECT_AUTO_ID_COLUMN).getName().equals(FieldsMappings.CONNECT_AUTO_ID_COLUMN));
    assertTrue(mappings.get(0).getMappings().get(FieldsMappings.CONNECT_AUTO_ID_COLUMN).isPrimaryKey());

    assertTrue(mappings.get(1).getMappings().size() == 1); //two pks
    assertTrue(mappings.get(1).getMappings().get("f3").getName().equals("f3"));
    assertTrue(mappings.get(1).getMappings().get("f3").isPrimaryKey());
  }

  @Test(expected = ConfigException.class)
  public void UpsertThrowBatchingSelectFieldsAutoCreatePKNotInSelected() {
    TestBase base = new TestBase();
    Map<String, String> props = base.getPropsSelectedFieldsAutoCreatePKBad();
    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings.from(config);
  }


  @Test(expected = ConfigException.class)
  public void throwTheExceptionMissingTable() {

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
    props.put(EXPORT_MAPPINGS, bad);

    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings.from(config);
  }

  @Test(expected = ConfigException.class)
  public void throwTheExceptionMissingFields() {

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
    props.put(EXPORT_MAPPINGS, bad);

    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings.from(config);
  }
}