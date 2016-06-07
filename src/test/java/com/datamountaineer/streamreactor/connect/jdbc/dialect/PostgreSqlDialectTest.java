package com.datamountaineer.streamreactor.connect.jdbc.dialect;

import com.datamountaineer.streamreactor.connect.jdbc.sink.SinkRecordField;
import com.google.common.collect.Lists;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by andrew@datamountaineer.com on 17/05/16.
 * kafka-connect-jdbc
 */
public class PostgreSqlDialectTest {

  private final DbDialect dialect = new PostgreSQLDialect();

  @Test(expected = IllegalArgumentException.class)
  public void throwAnExceptionIfTableIsNull() {
    dialect.getUpsertQuery(null, Lists.newArrayList("value"), Lists.newArrayList("id"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwAnExceptionIfTableNameIsEmptyString() {
    dialect.getUpsertQuery("  ", Lists.newArrayList("value"), Lists.newArrayList("id"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwAnExceptionIfKeyColsIsNull() {
    dialect.getUpsertQuery("Person", Lists.newArrayList("value"), null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwAnExceptionIfKeyColsIsNullIsEmpty() {
    dialect.getUpsertQuery("Customer", Lists.newArrayList("value"), Lists.<String>newArrayList());
  }

  @Test
  public void produceTheUpsertQuery() {
    String expected = "INSERT INTO \"Customer\" (\"name\",\"salary\",\"address\",\"id\") VALUES (?,?,?,?) ON CONFLICT (\"id\") DO UPDATE SET " +
            "\"name\"=EXCLUDED.\"name\",\"salary\"=EXCLUDED.\"salary\",\"address\"=EXCLUDED.\"address\"";
    String insert = dialect.getUpsertQuery("Customer", Lists.newArrayList("name", "salary", "address"), Lists.newArrayList("id"));
    assertEquals(expected, insert);
  }


  @Test
  public void handleCreateTableMultiplePKColumns() {
    String actual = dialect.getCreateQuery("tableA", Lists.newArrayList(
            new SinkRecordField(Schema.Type.INT32, "userid", true),
            new SinkRecordField(Schema.Type.INT32, "userdataid", true),
            new SinkRecordField(Schema.Type.STRING, "info", false)
    ));

    String expected = "CREATE TABLE \"tableA\" (" + System.lineSeparator() +
            "\"userid\" INT NOT NULL," + System.lineSeparator() +
            "\"userdataid\" INT NOT NULL," + System.lineSeparator() +
            "\"info\" TEXT NULL," + System.lineSeparator() +
            "PRIMARY KEY(\"userid\",\"userdataid\"))";
    assertEquals(expected, actual);
  }

  @Test
  public void handleCreateTableOnePKColumn() {
    String actual = dialect.getCreateQuery("tableA", Lists.newArrayList(
            new SinkRecordField(Schema.Type.INT32, "col1", true),
            new SinkRecordField(Schema.Type.INT64, "col2", false),
            new SinkRecordField(Schema.Type.STRING, "col3", false),
            new SinkRecordField(Schema.Type.FLOAT32, "col4", false),
            new SinkRecordField(Schema.Type.FLOAT64, "col5", false),
            new SinkRecordField(Schema.Type.BOOLEAN, "col6", false),
            new SinkRecordField(Schema.Type.INT8, "col7", false),
            new SinkRecordField(Schema.Type.INT16, "col8", false)
    ));

    String expected = "CREATE TABLE \"tableA\" (" + System.lineSeparator() +
            "\"col1\" INT NOT NULL," + System.lineSeparator() +
            "\"col2\" BIGINT NULL," + System.lineSeparator() +
            "\"col3\" TEXT NULL," + System.lineSeparator() +
            "\"col4\" FLOAT NULL," + System.lineSeparator() +
            "\"col5\" DOUBLE PRECISION NULL," + System.lineSeparator() +
            "\"col6\" BOOLEAN NULL," + System.lineSeparator() +
            "\"col7\" SMALLINT NULL," + System.lineSeparator() +
            "\"col8\" SMALLINT NULL," + System.lineSeparator() +
            "PRIMARY KEY(\"col1\"))";
    assertEquals(expected, actual);
  }

  @Test
  public void handleCreateTableNoPKColumn() {
    String actual = dialect.getCreateQuery("tableA", Lists.newArrayList(
            new SinkRecordField(Schema.Type.INT32, "col1", false),
            new SinkRecordField(Schema.Type.INT64, "col2", false),
            new SinkRecordField(Schema.Type.STRING, "col3", false),
            new SinkRecordField(Schema.Type.FLOAT32, "col4", false),
            new SinkRecordField(Schema.Type.FLOAT64, "col5", false),
            new SinkRecordField(Schema.Type.BOOLEAN, "col6", false),
            new SinkRecordField(Schema.Type.INT8, "col7", false),
            new SinkRecordField(Schema.Type.INT16, "col8", false)
    ));

    String expected = "CREATE TABLE \"tableA\" (" + System.lineSeparator() +
            "\"col1\" INT NULL," + System.lineSeparator() +
            "\"col2\" BIGINT NULL," + System.lineSeparator() +
            "\"col3\" TEXT NULL," + System.lineSeparator() +
            "\"col4\" FLOAT NULL," + System.lineSeparator() +
            "\"col5\" DOUBLE PRECISION NULL," + System.lineSeparator() +
            "\"col6\" BOOLEAN NULL," + System.lineSeparator() +
            "\"col7\" SMALLINT NULL," + System.lineSeparator() +
            "\"col8\" SMALLINT NULL)";
    assertEquals(expected, actual);
  }

  @Test
  public void handleAmendAddColumns() {
    List<String> actual = dialect.getAlterTable("tableA", Lists.newArrayList(
            new SinkRecordField(Schema.Type.INT32, "col1", false),
            new SinkRecordField(Schema.Type.INT64, "col2", false),
            new SinkRecordField(Schema.Type.STRING, "col3", false),
            new SinkRecordField(Schema.Type.FLOAT32, "col4", false),
            new SinkRecordField(Schema.Type.FLOAT64, "col5", false),
            new SinkRecordField(Schema.Type.BOOLEAN, "col6", false),
            new SinkRecordField(Schema.Type.INT8, "col7", false),
            new SinkRecordField(Schema.Type.INT16, "col8", false)
    ));

    assertEquals(1, actual.size());

    String expected = "ALTER TABLE \"tableA\" " + System.lineSeparator() +
            "ADD COLUMN \"col1\" INT NULL," + System.lineSeparator() +
            "ADD COLUMN \"col2\" BIGINT NULL," + System.lineSeparator() +
            "ADD COLUMN \"col3\" TEXT NULL," + System.lineSeparator() +
            "ADD COLUMN \"col4\" FLOAT NULL," + System.lineSeparator() +
            "ADD COLUMN \"col5\" DOUBLE PRECISION NULL," + System.lineSeparator() +
            "ADD COLUMN \"col6\" BOOLEAN NULL," + System.lineSeparator() +
            "ADD COLUMN \"col7\" SMALLINT NULL," + System.lineSeparator() +
            "ADD COLUMN \"col8\" SMALLINT NULL";
    assertEquals(expected, actual.get(0));
  }
}
