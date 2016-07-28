/*
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.jdbc.sink.dialect;

import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;

import static org.junit.Assert.assertEquals;

public class PostgreSqlDialectTest {

  private final DbDialect dialect = new PostgreSqlDialect();

  @Test
  public void produceTheUpsertQuery() {
    String expected = "INSERT INTO \"Customer\" (\"id\",\"name\",\"salary\",\"address\") VALUES (?,?,?,?) ON CONFLICT (\"id\") DO UPDATE SET " +
                      "\"name\"=EXCLUDED.\"name\",\"salary\"=EXCLUDED.\"salary\",\"address\"=EXCLUDED.\"address\"";
    String insert = dialect.getUpsertQuery("Customer", Collections.singletonList("id"), Arrays.asList("name", "salary", "address"));
    assertEquals(expected, insert);
  }

  @Test
  public void handleCreateTableMultiplePKColumns() {
    String actual = dialect.getCreateQuery("tableA", Arrays.asList(
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
    String actual = dialect.getCreateQuery("tableA", Arrays.asList(
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
                      "\"col4\" REAL NULL," + System.lineSeparator() +
                      "\"col5\" DOUBLE PRECISION NULL," + System.lineSeparator() +
                      "\"col6\" BOOLEAN NULL," + System.lineSeparator() +
                      "\"col7\" SMALLINT NULL," + System.lineSeparator() +
                      "\"col8\" SMALLINT NULL," + System.lineSeparator() +
                      "PRIMARY KEY(\"col1\"))";
    assertEquals(expected, actual);
  }

  @Test
  public void handleCreateTableNoPKColumn() {
    String actual = dialect.getCreateQuery("tableA", Arrays.asList(
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
                      "\"col4\" REAL NULL," + System.lineSeparator() +
                      "\"col5\" DOUBLE PRECISION NULL," + System.lineSeparator() +
                      "\"col6\" BOOLEAN NULL," + System.lineSeparator() +
                      "\"col7\" SMALLINT NULL," + System.lineSeparator() +
                      "\"col8\" SMALLINT NULL)";
    assertEquals(expected, actual);
  }

  @Test
  public void handleAmendAddColumns() {
    List<String> actual = dialect.getAlterTable("tableA", Arrays.asList(
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
                      "ADD \"col1\" INT NULL," + System.lineSeparator() +
                      "ADD \"col2\" BIGINT NULL," + System.lineSeparator() +
                      "ADD \"col3\" TEXT NULL," + System.lineSeparator() +
                      "ADD \"col4\" REAL NULL," + System.lineSeparator() +
                      "ADD \"col5\" DOUBLE PRECISION NULL," + System.lineSeparator() +
                      "ADD \"col6\" BOOLEAN NULL," + System.lineSeparator() +
                      "ADD \"col7\" SMALLINT NULL," + System.lineSeparator() +
                      "ADD \"col8\" SMALLINT NULL";
    assertEquals(expected, actual.get(0));
  }
}
