/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.jdbc.dialect;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Test;

import java.util.List;

import io.confluent.connect.jdbc.util.TableId;

import static org.junit.Assert.assertEquals;

public class OracleDatabaseDialectTest extends BaseDialectTest<OracleDatabaseDialect> {

  @Override
  protected OracleDatabaseDialect createDialect() {
    return new OracleDatabaseDialect(sourceConfigWithUrl("jdbc:oracle:thin://something"));
  }

  @Test
  public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
    assertPrimitiveMapping(Type.INT8, "NUMBER(3,0)");
    assertPrimitiveMapping(Type.INT16, "NUMBER(5,0)");
    assertPrimitiveMapping(Type.INT32, "NUMBER(10,0)");
    assertPrimitiveMapping(Type.INT64, "NUMBER(19,0)");
    assertPrimitiveMapping(Type.FLOAT32, "BINARY_FLOAT");
    assertPrimitiveMapping(Type.FLOAT64, "BINARY_DOUBLE");
    assertPrimitiveMapping(Type.BOOLEAN, "NUMBER(1,0)");
    assertPrimitiveMapping(Type.BYTES, "BLOB");
    assertPrimitiveMapping(Type.STRING, "CLOB");
  }

  @Test
  public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
    assertDecimalMapping(0, "NUMBER(*,0)");
    assertDecimalMapping(3, "NUMBER(*,3)");
    assertDecimalMapping(4, "NUMBER(*,4)");
    assertDecimalMapping(5, "NUMBER(*,5)");
  }

  @Test
  public void shouldMapDataTypes() {
    verifyDataTypeMapping("NUMBER(3,0)", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("NUMBER(5,0)", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("NUMBER(10,0)", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("NUMBER(19,0)", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("BINARY_FLOAT", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("BINARY_DOUBLE", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("NUMBER(1,0)", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("CLOB", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("BLOB", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("NUMBER(*,0)", Decimal.schema(0));
    verifyDataTypeMapping("NUMBER(*,42)", Decimal.schema(42));
    verifyDataTypeMapping("DATE", Date.SCHEMA);
    verifyDataTypeMapping("DATE", Time.SCHEMA);
    verifyDataTypeMapping("TIMESTAMP", Timestamp.SCHEMA);
  }

  @Test
  public void shouldMapDateSchemaTypeToDateSqlType() {
    assertDateMapping("DATE");
  }

  @Test
  public void shouldMapTimeSchemaTypeToTimeSqlType() {
    assertTimeMapping("DATE");
  }

  @Test
  public void shouldMapTimestampSchemaTypeToTimestampSqlType() {
    assertTimestampMapping("TIMESTAMP");
  }

  @Test
  public void shouldBuildCreateQueryStatement() {
    String expected = "CREATE TABLE \"myTable\" (\n" + "\"c1\" NUMBER(10,0) NOT NULL,\n" +
                      "\"c2\" NUMBER(19,0) NOT NULL,\n" + "\"c3\" CLOB NOT NULL,\n" +
                      "\"c4\" CLOB NULL,\n" + "\"c5\" DATE DEFAULT '2001-03-15',\n" +
                      "\"c6\" DATE DEFAULT '00:00:00.000',\n" +
                      "\"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n" +
                      "\"c8\" NUMBER(*,4) NULL,\n" + "PRIMARY KEY(\"c1\"))";
    String sql = dialect.buildCreateTableStatement(tableId, sinkRecordFields);
    assertEquals(expected, sql);
  }

  @Test
  public void shouldBuildAlterTableStatement() {
    List<String> statements = dialect.buildAlterTable(tableId, sinkRecordFields);
    String[] sql = {"ALTER TABLE \"myTable\" ADD(\n" + "\"c1\" NUMBER(10,0) NOT NULL,\n" +
                    "\"c2\" NUMBER(19,0) NOT NULL,\n" + "\"c3\" CLOB NOT NULL,\n" +
                    "\"c4\" CLOB NULL,\n" + "\"c5\" DATE DEFAULT '2001-03-15',\n" +
                    "\"c6\" DATE DEFAULT '00:00:00.000',\n" +
                    "\"c7\" TIMESTAMP DEFAULT '2001-03-15 00:00:00.000',\n" +
                    "\"c8\" NUMBER(*,4) NULL)"};
    assertStatements(sql, statements);
  }

  @Test
  public void shouldBuildUpsertStatement() {
    String expected = "merge into \"myTable\" using (select ? \"id1\", ? \"id2\", ? \"columnA\", " +
                      "? \"columnB\", ? \"columnC\", ? \"columnD\" FROM dual) incoming on" +
                      "(\"myTable\".\"id1\"=incoming.\"id1\" and \"myTable\".\"id2\"=incoming" +
                      ".\"id2\") when matched then update set \"myTable\".\"columnA\"=incoming" +
                      ".\"columnA\",\"myTable\".\"columnB\"=incoming.\"columnB\",\"myTable\"" +
                      ".\"columnC\"=incoming.\"columnC\",\"myTable\".\"columnD\"=incoming" +
                      ".\"columnD\" when not matched then insert(\"myTable\".\"columnA\"," +
                      "\"myTable\".\"columnB\",\"myTable\".\"columnC\",\"myTable\".\"columnD\"," +
                      "\"myTable\".\"id1\",\"myTable\".\"id2\") values(incoming.\"columnA\"," +
                      "incoming.\"columnB\",incoming.\"columnC\",incoming.\"columnD\",incoming" +
                      ".\"id1\",incoming.\"id2\")";
    String sql = dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
    assertEquals(expected, sql);
  }

  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"col1\" NUMBER(10,0) NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" NUMBER(10,0) NOT NULL," +
        System.lineSeparator() + "PRIMARY KEY(\"pk1\"))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE \"myTable\" (" + System.lineSeparator() + "\"pk1\" NUMBER(10,0) NOT NULL," +
        System.lineSeparator() + "\"pk2\" NUMBER(10,0) NOT NULL," + System.lineSeparator() +
        "\"col1\" NUMBER(10,0) NOT NULL," + System.lineSeparator() +
        "PRIMARY KEY(\"pk1\",\"pk2\"))");
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol(
        "ALTER TABLE \"myTable\" ADD(" + System.lineSeparator() + "\"newcol1\" NUMBER(10,0) NULL)");
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols(
        "ALTER TABLE \"myTable\" ADD(" + System.lineSeparator() +
        "\"newcol1\" NUMBER(10,0) NULL," +
        System.lineSeparator() + "\"newcol2\" NUMBER(10,0) DEFAULT 42)");
  }

  @Test
  public void upsert() {
    TableId article = tableId("ARTICLE");
    String expected = "merge into \"ARTICLE\" " +
                      "using (select ? \"title\", ? \"author\", ? \"body\" FROM dual) incoming on" +
                      "(\"ARTICLE\".\"title\"=incoming.\"title\" and \"ARTICLE\"" +
                      ".\"author\"=incoming.\"author\") " +
                      "when matched then update set \"ARTICLE\".\"body\"=incoming.\"body\" " +
                      "when not matched then insert(\"ARTICLE\".\"body\",\"ARTICLE\".\"title\"," +
                      "\"ARTICLE\".\"author\") " +
                      "values(incoming.\"body\",incoming.\"title\",incoming.\"author\")";
    String actual = dialect.buildUpsertQueryStatement(article, columns(article, "title", "author"),
                                                      columns(article, "body"));
    assertEquals(expected, actual);
  }

}