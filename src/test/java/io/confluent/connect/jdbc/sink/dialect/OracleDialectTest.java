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

public class OracleDialectTest {
  private final OracleDialect dialect = new OracleDialect();

  @Test
  public void handleCreateTableMultiplePKColumns() {
    String actual = dialect.getCreateQuery("tableA", Arrays.asList(
        new SinkRecordField(Schema.Type.INT32, "userid", true),
        new SinkRecordField(Schema.Type.INT32, "userdataid", true),
        new SinkRecordField(Schema.Type.STRING, "info", false)
    ));

    String expected = "CREATE TABLE \"tableA\" (" + System.lineSeparator() +
                      "\"userid\" NUMBER(10,0) NOT NULL," + System.lineSeparator() +
                      "\"userdataid\" NUMBER(10,0) NOT NULL," + System.lineSeparator() +
                      "\"info\" NVARCHAR2(4000) NULL," + System.lineSeparator() +
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
                      "\"col1\" NUMBER(10,0) NOT NULL," + System.lineSeparator() +
                      "\"col2\" NUMBER(19,0) NULL," + System.lineSeparator() +
                      "\"col3\" NVARCHAR2(4000) NULL," + System.lineSeparator() +
                      "\"col4\" BINARY_FLOAT NULL," + System.lineSeparator() +
                      "\"col5\" BINARY_DOUBLE NULL," + System.lineSeparator() +
                      "\"col6\" NUMBER(1,0) NULL," + System.lineSeparator() +
                      "\"col7\" NUMBER(3,0) NULL," + System.lineSeparator() +
                      "\"col8\" NUMBER(5,0) NULL," + System.lineSeparator() +
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
                      "\"col1\" NUMBER(10,0) NULL," + System.lineSeparator() +
                      "\"col2\" NUMBER(19,0) NULL," + System.lineSeparator() +
                      "\"col3\" NVARCHAR2(4000) NULL," + System.lineSeparator() +
                      "\"col4\" BINARY_FLOAT NULL," + System.lineSeparator() +
                      "\"col5\" BINARY_DOUBLE NULL," + System.lineSeparator() +
                      "\"col6\" NUMBER(1,0) NULL," + System.lineSeparator() +
                      "\"col7\" NUMBER(3,0) NULL," + System.lineSeparator() +
                      "\"col8\" NUMBER(5,0) NULL)";
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

    String expected = "ALTER TABLE \"tableA\" ADD(" + System.lineSeparator() +
                      "\"col1\" NUMBER(10,0) NULL," + System.lineSeparator() +
                      "\"col2\" NUMBER(19,0) NULL," + System.lineSeparator() +
                      "\"col3\" NVARCHAR2(4000) NULL," + System.lineSeparator() +
                      "\"col4\" BINARY_FLOAT NULL," + System.lineSeparator() +
                      "\"col5\" BINARY_DOUBLE NULL," + System.lineSeparator() +
                      "\"col6\" NUMBER(1,0) NULL," + System.lineSeparator() +
                      "\"col7\" NUMBER(3,0) NULL," + System.lineSeparator() +
                      "\"col8\" NUMBER(5,0) NULL)";
    assertEquals(expected, actual.get(0));
  }

  @Test
  public void createTheUpsertStatement() {
    String expected = "merge into \"ARTICLE\" " +
                      "using (select ? \"title\", ? \"author\", ? \"body\" FROM dual) incoming on" +
                      "(\"ARTICLE\".\"title\"=incoming.\"title\" and \"ARTICLE\".\"author\"=incoming.\"author\") " +
                      "when matched then update set \"ARTICLE\".\"body\"=incoming.\"body\" " +
                      "when not matched then insert(\"ARTICLE\".\"body\",\"ARTICLE\".\"title\",\"ARTICLE\".\"author\") " +
                      "values(incoming.\"body\",incoming.\"title\",incoming.\"author\")";

    String upsert = dialect.getUpsertQuery("ARTICLE",
                                           Arrays.asList("title", "author"), Collections.singletonList("body")
    );

    assertEquals(expected, upsert);
  }
}
