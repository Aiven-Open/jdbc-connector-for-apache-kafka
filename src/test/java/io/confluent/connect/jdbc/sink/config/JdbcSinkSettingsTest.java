package io.confluent.connect.jdbc.sink.config;

import org.apache.curator.test.InstanceSpec;
import org.apache.kafka.common.config.ConfigException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.sink.TestBase;
import io.confluent.connect.jdbc.sink.services.EmbeddedSingleNodeKafkaCluster;
import io.confluent.connect.jdbc.sink.services.RestApp;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JdbcSinkSettingsTest {

  private static final String DB_FILE = "test_db_writer_sqllite.db";
  private static final String SQL_LITE_URI = "jdbc:sqlite:" + DB_FILE;
  private int port;
  private final TestBase base = new TestBase();
  private RestService client;


  @Before
  public void setUp() throws Exception {
    deleteSqlLiteFile();
    port = InstanceSpec.getRandomPort();
    EmbeddedSingleNodeKafkaCluster cluster = new EmbeddedSingleNodeKafkaCluster();
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
  public void InsertBatchingAllFields() {
    Map<String, String> props = base.getPropsAllFields("insert", false);

    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings settings = JdbcSinkSettings.from(config);
    for (FieldsMappings fm : settings.getMappings()) {
      assertTrue(fm.getInsertMode().equals(InsertModeEnum.INSERT));
    }
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
  public void InsertBatchingSelectedFields() {
    Map<String, String> props = base.getPropsSelectedFields("insert", false);

    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings settings = JdbcSinkSettings.from(config);
    for (FieldsMappings fm : settings.getMappings()) {
      assertTrue(fm.getInsertMode().equals(InsertModeEnum.INSERT));
    }

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
  public void UpsertBatchingAllFields() {
    Map<String, String> props = base.getPropsAllFields("upsert", false);

    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings settings = JdbcSinkSettings.from(config);
    for (FieldsMappings fm : settings.getMappings()) {
      assertTrue(fm.getInsertMode().equals(InsertModeEnum.UPSERT));
    }

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
  public void UpsertBatchingSelectedFields() {
    Map<String, String> props = base.getPropsSelectedFields("upsert", false);

    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings settings = JdbcSinkSettings.from(config);
    for (FieldsMappings fm : settings.getMappings()) {
      assertTrue(fm.getInsertMode().equals(InsertModeEnum.UPSERT));
    }

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
  public void UpsertBatchingAllFieldsAutoCreate() throws IOException, RestClientException {
    Map<String, String> props = base.getPropsAllFieldsAutoCreatePK();

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
    client.registerSchema(rawSchema, base.getTopic1());
    props.put(JdbcSinkConfig.DATABASE_CONNECTION_URI, SQL_LITE_URI);

    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings settings = JdbcSinkSettings.from(config);
    for (FieldsMappings fm : settings.getMappings()) {
      assertTrue(fm.getInsertMode().equals(InsertModeEnum.UPSERT));
    }

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

    assertTrue(mappings.get(0).getMappings().size() == 2); //auto PK
    assertTrue(mappings.get(0).getMappings().get("f1").getName().equals("f1"));
    assertTrue(mappings.get(0).getMappings().get("f1").isPrimaryKey());
    assertTrue(mappings.get(0).getMappings().get("f2").getName().equals("f2"));
    assertTrue(mappings.get(0).getMappings().get("f2").isPrimaryKey());

    assertTrue(mappings.get(1).getMappings().size() == 1); //two pks
    assertTrue(mappings.get(1).getMappings().get("f3").getName().equals("f3"));
    assertTrue(mappings.get(1).getMappings().get("f3").isPrimaryKey());
  }

  @Test(expected = ConfigException.class)
  public void throwTheExceptionMissingTable() {
    Map<String, String> props = new HashMap<>();
    //missing target
    String bad = "{topic1:;*}";
    props.put(JdbcSinkConfig.DATABASE_CONNECTION_URI, "jdbc://");
    props.put(JdbcSinkConfig.EXPORT_MAPPINGS, bad);

    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings.from(config);
  }

  @Test(expected = ConfigException.class)
  public void throwTheExceptionMissingFields() {
    Map<String, String> props = new HashMap<>();
    //missing target
    String bad = "INSERT INTO table1 SELECT FROM topic1";
    props.put(JdbcSinkConfig.DATABASE_CONNECTION_URI, "jdbc://");
    props.put(JdbcSinkConfig.EXPORT_MAPPINGS, bad);

    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings.from(config);
  }

  @Test(expected = ConfigException.class)
  public void throwTheExceptionUpsertNoPK() {
    TestBase base = new TestBase();
    Map<String, String> props = base.getPropsSelectedFieldsAutoCreatePKBad();
    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings.from(config);
  }
}