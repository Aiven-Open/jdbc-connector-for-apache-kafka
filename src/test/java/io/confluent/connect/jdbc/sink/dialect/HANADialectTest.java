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

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class HANADialectTest {

  private final DbDialect dialect = new HANADialect();

  @Test
  public void handleCreateTableMultiplePKColumns() {
    String actual = dialect.getCreateQuery("tableA", Arrays.asList(
      new SinkRecordField(Schema.INT32_SCHEMA, "userid", true),
      new SinkRecordField(Schema.INT32_SCHEMA, "userdataid", true),
      new SinkRecordField(Schema.OPTIONAL_STRING_SCHEMA, "info", false)
    ));

    String expected = "CREATE COLUMN TABLE \"tableA\" (" + System.lineSeparator() +
                      "\"userid\" INTEGER NOT NULL," + System.lineSeparator() +
                      "\"userdataid\" INTEGER NOT NULL," + System.lineSeparator() +
                      "\"info\" VARCHAR(1000) NULL," + System.lineSeparator() +
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
      new SinkRecordField(Schema.OPTIONAL_INT8_SCHEMA, "col6", false),
      new SinkRecordField(Schema.OPTIONAL_INT16_SCHEMA, "col7", false)
    ));

    String expected = "CREATE COLUMN TABLE \"tableA\" (" + System.lineSeparator() +
                      "\"col1\" INTEGER NOT NULL," + System.lineSeparator() +
                      "\"col2\" BIGINT NULL," + System.lineSeparator() +
                      "\"col3\" VARCHAR(1000) NULL," + System.lineSeparator() +
                      "\"col4\" REAL NULL," + System.lineSeparator() +
                      "\"col5\" DOUBLE NULL," + System.lineSeparator() +
                      "\"col6\" TINYINT NULL," + System.lineSeparator() +
                      "\"col7\" SMALLINT NULL," + System.lineSeparator() +
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
        new SinkRecordField(Schema.OPTIONAL_INT8_SCHEMA, "col6", false),
        new SinkRecordField(Schema.OPTIONAL_INT16_SCHEMA, "col7", false)
    ));

    String expected = "CREATE COLUMN TABLE \"tableA\" (" + System.lineSeparator() +
                      "\"col1\" INTEGER NULL," + System.lineSeparator() +
                      "\"col2\" BIGINT NULL," + System.lineSeparator() +
                      "\"col3\" VARCHAR(1000) NULL," + System.lineSeparator() +
                      "\"col4\" REAL NULL," + System.lineSeparator() +
                      "\"col5\" DOUBLE NULL," + System.lineSeparator() +
                      "\"col6\" TINYINT NULL," + System.lineSeparator() +
                      "\"col7\" SMALLINT NULL)";

    assertEquals(expected, actual);
  }

  @Test
  public void handleAmendAddColumns() {
    List<String> actual = dialect.getAlterTable("tableA", Arrays.asList(
        new SinkRecordField(Schema.INT32_SCHEMA, "col1", false),
        new SinkRecordField(Schema.OPTIONAL_INT64_SCHEMA, "col2", false),
        new SinkRecordField(Schema.OPTIONAL_STRING_SCHEMA, "col3", false),
        new SinkRecordField(Schema.OPTIONAL_FLOAT32_SCHEMA, "col4", false),
        new SinkRecordField(Schema.OPTIONAL_FLOAT64_SCHEMA, "col5", false),
        new SinkRecordField(Schema.OPTIONAL_INT8_SCHEMA, "col6", false),
        new SinkRecordField(Schema.OPTIONAL_INT16_SCHEMA, "col7", false)
    ));

    assertEquals(1, actual.size());

    String expected = "ALTER TABLE \"tableA\" ADD(" + System.lineSeparator() +
                      "\"col1\" INTEGER NOT NULL," + System.lineSeparator() +
                      "\"col2\" BIGINT NULL," + System.lineSeparator() +
                      "\"col3\" VARCHAR(1000) NULL," + System.lineSeparator() +
                      "\"col4\" REAL NULL," + System.lineSeparator() +
                      "\"col5\" DOUBLE NULL," + System.lineSeparator() +
                      "\"col6\" TINYINT NULL," + System.lineSeparator() +
                      "\"col7\" SMALLINT NULL)";

    assertEquals(expected, actual.get(0));
  }

   @Test
   public void createTheUpsertQuery() {
       String expected = "UPSERT \"tableA\"(\"col1\",\"col2\",\"col3\",\"col4\") " +
                "VALUES(?,?,?,?) WITH PRIMARY KEY";

        String upsert = dialect.getUpsertQuery("tableA",
                Arrays.asList("col1"), Arrays.asList("col2","col3","col4")
        );
        assertEquals(expected, upsert);
    }
}