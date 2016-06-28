package io.confluent.connect.jdbc.sink.avro;

import org.apache.curator.test.InstanceSpec;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;

import io.confluent.connect.jdbc.sink.SinkRecordField;
import io.confluent.connect.jdbc.sink.config.FieldAlias;
import io.confluent.connect.jdbc.sink.dialect.DbDialect;
import io.confluent.connect.jdbc.sink.dialect.SQLiteDialect;
import io.confluent.connect.jdbc.sink.services.EmbeddedSingleNodeKafkaCluster;
import io.confluent.connect.jdbc.sink.services.RestApp;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;

import static org.junit.Assert.assertEquals;

public class AvroToFieldConverterTest {

  @Test
  public void ConverterTest() throws Exception {

    int port = InstanceSpec.getRandomPort();
    EmbeddedSingleNodeKafkaCluster cluster = new EmbeddedSingleNodeKafkaCluster();
    RestApp registry = new RestApp(port, cluster.zookeeperConnect(), "converterTest");
    registry.start();
    RestService client = registry.restClient;

    String rawSchema = "{\n" +
                       "\t\"type\": \"record\",\n" +
                       "\t\"name\": \"myrecord\",\n" +
                       "\t\"fields\": [{\n" +
                       "\t\t\"name\": \"id\",\n" +
                       "\t\t\"type\": \"int\"\n" +
                       "\t}, {\n" +
                       "\t\t\"name\": \"stringType\",\n" +
                       "\t\t\"type\": \"string\"\n" +
                       "\t}, {\n" +
                       "\t\t\"name\": \"intType\",\n" +
                       "\t\t\"type\": \"int\"\n" +
                       "\t}, {\n" +
                       "\t\t\"name\": \"floatType\",\n" +
                       "\t\t\"type\": \"float\"\n" +
                       "\t}, {\n" +
                       "\n" +
                       "\t\t\"name\": \"longType\",\n" +
                       "\t\t\"type\": \"long\"\n" +
                       "\t}, {\n" +
                       "\n" +
                       "\t\t\"name\": \"doubleType\",\n" +
                       "\t\t\"type\": \"double\"\n" +
                       "\t}, {\n" +
                       "\n" +
                       "\t\t\"name\": \"booleanType\",\n" +
                       "\t\t\"type\": \"boolean\"\n" +
                       "\t}, {\n" +
                       "\n" +
                       "\t\t\"name\": \"unionType\",\n" +
                       "\t\t\"type\": [\"null\", \"int\"]\n" +
                       "\t}]\n" +
                       "}";

    client.registerSchema(rawSchema, "converterTest");

    Schema latest = client.getLatestVersion("converterTest");

    String ddlString = "CREATE TABLE `test` (\n" +
                       "`id` NUMERIC NULL,\n" +
                       "`stringType` TEXT NULL,\n" +
                       "`intType` NUMERIC NULL,\n" +
                       "`floatType` REAL NULL,\n" +
                       "`longType` NUMERIC NULL,\n" +
                       "`doubleType` REAL NULL,\n" +
                       "`booleanType` NUMERIC NULL,\n" +
                       "`unionType` NUMERIC NULL);";

    AvroToDbConverter converter = new AvroToDbConverter();
    Collection<SinkRecordField> fields = converter.convert(latest.getSchema(), new HashMap<String, FieldAlias>());
    DbDialect db = new SQLiteDialect();
    String ddl = db.getCreateQuery("test", fields);
    assertEquals(ddlString, ddl);
  }
}
