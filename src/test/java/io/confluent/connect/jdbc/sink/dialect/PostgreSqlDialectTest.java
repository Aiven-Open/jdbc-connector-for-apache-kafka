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

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;

import static org.junit.Assert.assertEquals;

public class PostgreSqlDialectTest {

  private final DbDialect dialect = new PostgreSqlDialect();

  @Test
  public void formatColumnValue() {
    verifyFormatColumnValue(Decimal.LOGICAL_NAME, new BigDecimal("42.42"), "42.42");

    final java.util.Date instant = new java.util.Date(1474661402);
    verifyFormatColumnValue(Date.LOGICAL_NAME, instant, "to_timestamp(1474661402::double precision / 1000)::date");
    verifyFormatColumnValue(Time.LOGICAL_NAME, instant, "to_timestamp(1474661402::double precision / 1000)::time");
    verifyFormatColumnValue(Timestamp.LOGICAL_NAME, instant, "to_timestamp(1474661402::double precision / 1000)::timestamp");
  }

  private void verifyFormatColumnValue(String schemaName, Object value, String expected) {
    final StringBuilder builder = new StringBuilder();
    dialect.formatColumnValue(builder, schemaName, null, null, value);
    assertEquals(expected, builder.toString());
  }

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
        new SinkRecordField(Schema.INT32_SCHEMA, "userid", true),
        new SinkRecordField(Schema.INT32_SCHEMA, "userdataid", true),
        new SinkRecordField(Schema.OPTIONAL_STRING_SCHEMA, "info", false)
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
        new SinkRecordField(Schema.OPTIONAL_INT32_SCHEMA, "col1", true),
        new SinkRecordField(Schema.OPTIONAL_INT64_SCHEMA, "col2", false),
        new SinkRecordField(Schema.OPTIONAL_STRING_SCHEMA, "col3", false),
        new SinkRecordField(Schema.OPTIONAL_FLOAT32_SCHEMA, "col4", false),
        new SinkRecordField(Schema.OPTIONAL_FLOAT64_SCHEMA, "col5", false),
        new SinkRecordField(Schema.OPTIONAL_BOOLEAN_SCHEMA, "col6", false),
        new SinkRecordField(Schema.OPTIONAL_INT8_SCHEMA, "col7", false),
        new SinkRecordField(Schema.OPTIONAL_INT16_SCHEMA, "col8", false),
        new SinkRecordField(Timestamp.builder().optional().build(), "col9", false),
        new SinkRecordField(Date.builder().optional().build(), "col10", false),
        new SinkRecordField(Time.builder().optional().build(), "col11", false),
        new SinkRecordField(Decimal.builder(0).optional().build(), "col12", false)
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
                      "\"col9\" TIMESTAMP NULL," + System.lineSeparator() +
                      "\"col10\" DATE NULL," + System.lineSeparator() +
                      "\"col11\" TIME NULL," + System.lineSeparator() +
                      "\"col12\" DECIMAL NULL," + System.lineSeparator() +
                      "PRIMARY KEY(\"col1\"))";
    assertEquals(expected, actual);
  }

  @Test
  public void handleCreateTableNoPKColumn() {
    String actual = dialect.getCreateQuery("tableA", Arrays.asList(
        new SinkRecordField(Schema.OPTIONAL_INT32_SCHEMA, "col1", false),
        new SinkRecordField(Schema.OPTIONAL_INT64_SCHEMA, "col2", false),
        new SinkRecordField(Schema.OPTIONAL_STRING_SCHEMA, "col3", false),
        new SinkRecordField(Schema.OPTIONAL_FLOAT32_SCHEMA, "col4", false),
        new SinkRecordField(Schema.OPTIONAL_FLOAT64_SCHEMA, "col5", false),
        new SinkRecordField(Schema.OPTIONAL_BOOLEAN_SCHEMA, "col6", false),
        new SinkRecordField(Schema.OPTIONAL_INT8_SCHEMA, "col7", false),
        new SinkRecordField(Schema.OPTIONAL_INT16_SCHEMA, "col8", false),
        new SinkRecordField(Timestamp.builder().optional().build(), "col9", false),
        new SinkRecordField(Date.builder().optional().build(), "col10", false),
        new SinkRecordField(Time.builder().optional().build(), "col11", false),
        new SinkRecordField(Decimal.builder(0).optional().build(), "col12", false)
    ));

    String expected = "CREATE TABLE \"tableA\" (" + System.lineSeparator() +
                      "\"col1\" INT NULL," + System.lineSeparator() +
                      "\"col2\" BIGINT NULL," + System.lineSeparator() +
                      "\"col3\" TEXT NULL," + System.lineSeparator() +
                      "\"col4\" REAL NULL," + System.lineSeparator() +
                      "\"col5\" DOUBLE PRECISION NULL," + System.lineSeparator() +
                      "\"col6\" BOOLEAN NULL," + System.lineSeparator() +
                      "\"col7\" SMALLINT NULL," + System.lineSeparator() +
                      "\"col8\" SMALLINT NULL," + System.lineSeparator() +
                      "\"col9\" TIMESTAMP NULL," + System.lineSeparator() +
                      "\"col10\" DATE NULL," + System.lineSeparator() +
                      "\"col11\" TIME NULL," + System.lineSeparator() +
                      "\"col12\" DECIMAL NULL)";
    assertEquals(expected, actual);
  }

  @Test
  public void handleAmendAddColumns() {
    List<String> actual = dialect.getAlterTable("tableA", Arrays.asList(
        new SinkRecordField(Schema.OPTIONAL_INT32_SCHEMA, "col1", false),
        new SinkRecordField(Schema.OPTIONAL_INT64_SCHEMA, "col2", false),
        new SinkRecordField(Schema.OPTIONAL_STRING_SCHEMA, "col3", false),
        new SinkRecordField(Schema.OPTIONAL_FLOAT32_SCHEMA, "col4", false),
        new SinkRecordField(Schema.OPTIONAL_FLOAT64_SCHEMA, "col5", false),
        new SinkRecordField(Schema.OPTIONAL_BOOLEAN_SCHEMA, "col6", false),
        new SinkRecordField(Schema.OPTIONAL_INT8_SCHEMA, "col7", false),
        new SinkRecordField(Schema.OPTIONAL_INT16_SCHEMA, "col8", false),
        new SinkRecordField(Timestamp.builder().optional().build(), "col9", false),
        new SinkRecordField(Date.builder().optional().build(), "col10", false),
        new SinkRecordField(Time.builder().optional().build(), "col11", false),
        new SinkRecordField(Decimal.builder(0).optional().build(), "col12", false)
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
                      "ADD \"col8\" SMALLINT NULL," + System.lineSeparator() +
                      "ADD \"col9\" TIMESTAMP NULL," + System.lineSeparator() +
                      "ADD \"col10\" DATE NULL," + System.lineSeparator() +
                      "ADD \"col11\" TIME NULL," + System.lineSeparator() +
                      "ADD \"col12\" DECIMAL NULL";
    assertEquals(expected, actual.get(0));
  }
}
