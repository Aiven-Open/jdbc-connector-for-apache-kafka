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

public class SqlServerDatabaseDialectTest extends BaseDialectTest<SqlServerDatabaseDialect> {

  @Override
  protected SqlServerDatabaseDialect createDialect() {
    return new SqlServerDatabaseDialect(sourceConfigWithUrl("jdbc:sqlsserver://something"));
  }


  @Test
  public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
    assertPrimitiveMapping(Type.INT8, "tinyint");
    assertPrimitiveMapping(Type.INT16, "smallint");
    assertPrimitiveMapping(Type.INT32, "int");
    assertPrimitiveMapping(Type.INT64, "bigint");
    assertPrimitiveMapping(Type.FLOAT32, "real");
    assertPrimitiveMapping(Type.FLOAT64, "float");
    assertPrimitiveMapping(Type.BOOLEAN, "bit");
    assertPrimitiveMapping(Type.BYTES, "varbinary(max)");
    assertPrimitiveMapping(Type.STRING, "varchar(max)");
  }

  @Test
  public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
    assertDecimalMapping(0, "decimal(38,0)");
    assertDecimalMapping(3, "decimal(38,3)");
    assertDecimalMapping(4, "decimal(38,4)");
    assertDecimalMapping(5, "decimal(38,5)");
  }

  @Test
  public void shouldMapDataTypes() {
    verifyDataTypeMapping("tinyint", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("smallint", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("int", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("bigint", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("real", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("float", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("bit", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("varchar(max)", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("varbinary(max)", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("decimal(38,0)", Decimal.schema(0));
    verifyDataTypeMapping("decimal(38,4)", Decimal.schema(4));
    verifyDataTypeMapping("date", Date.SCHEMA);
    verifyDataTypeMapping("time", Time.SCHEMA);
    verifyDataTypeMapping("datetime2", Timestamp.SCHEMA);
  }

  @Test
  public void shouldMapDateSchemaTypeToDateSqlType() {
    assertDateMapping("date");
  }

  @Test
  public void shouldMapTimeSchemaTypeToTimeSqlType() {
    assertTimeMapping("time");
  }

  @Test
  public void shouldMapTimestampSchemaTypeToTimestampSqlType() {
    assertTimestampMapping("datetime2");
  }

  @Test
  public void shouldBuildCreateQueryStatement() {
    String expected =
        "CREATE TABLE [myTable] (\n" + "[c1] int NOT NULL,\n" + "[c2] bigint NOT NULL,\n" +
        "[c3] varchar(max) NOT NULL,\n" + "[c4] varchar(max) NULL,\n" +
        "[c5] date DEFAULT '2001-03-15',\n" + "[c6] time DEFAULT '00:00:00.000',\n" +
        "[c7] datetime2 DEFAULT '2001-03-15 00:00:00.000',\n" + "[c8] decimal(38,4) NULL,\n" +
        "PRIMARY KEY([c1]))";
    String sql = dialect.buildCreateTableStatement(tableId, sinkRecordFields);
    assertEquals(expected, sql);
  }

  @Test
  public void shouldBuildAlterTableStatement() {
    List<String> statements = dialect.buildAlterTable(tableId, sinkRecordFields);
    String[] sql = {
        "ALTER TABLE [myTable] ADD\n" + "[c1] int NOT NULL,\n" + "[c2] bigint NOT NULL,\n" +
        "[c3] varchar(max) NOT NULL,\n" + "[c4] varchar(max) NULL,\n" +
        "[c5] date DEFAULT '2001-03-15',\n" + "[c6] time DEFAULT '00:00:00.000',\n" +
        "[c7] datetime2 DEFAULT '2001-03-15 00:00:00.000',\n" + "[c8] decimal(38,4) NULL"};
    assertStatements(sql, statements);
  }

  @Test
  public void shouldBuildUpsertStatement() {
    String expected = "merge into [myTable] with (HOLDLOCK) AS target using (select ? AS [id1], ?" +
                      " AS [id2], ? AS [columnA], ? AS [columnB], ? AS [columnC], ? AS [columnD])" +
                      " AS incoming on (target.[id1]=incoming.[id1] and target.[id2]=incoming" +
                      ".[id2]) when matched then update set [columnA]=incoming.[columnA]," +
                      "[columnB]=incoming.[columnB],[columnC]=incoming.[columnC]," +
                      "[columnD]=incoming.[columnD] when not matched then insert ([columnA], " +
                      "[columnB], [columnC], [columnD], [id1], [id2]) values (incoming.[columnA]," +
                      "incoming.[columnB],incoming.[columnC],incoming.[columnD],incoming.[id1]," +
                      "incoming.[id2]);";
    String sql = dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
    assertEquals(expected, sql);
  }

  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE [myTable] (" + System.lineSeparator() + "[col1] int NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE [myTable] (" + System.lineSeparator() + "[pk1] int NOT NULL," +
        System.lineSeparator() + "PRIMARY KEY([pk1]))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE [myTable] (" + System.lineSeparator() + "[pk1] int NOT NULL," +
        System.lineSeparator() + "[pk2] int NOT NULL," + System.lineSeparator() +
        "[col1] int NOT NULL," + System.lineSeparator() + "PRIMARY KEY([pk1],[pk2]))");
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol(
        "ALTER TABLE [myTable] ADD" + System.lineSeparator() + "[newcol1] int NULL");
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols(
        "ALTER TABLE [myTable] ADD" + System.lineSeparator() + "[newcol1] int NULL," +
        System.lineSeparator() + "[newcol2] int DEFAULT 42");
  }

  @Test
  public void upsert1() {
    TableId customer = tableId("Customer");
    assertEquals(
        "merge into [Customer] with (HOLDLOCK) AS target using (select ? AS [id], ? AS [name], ? " +
        "AS [salary], ? AS [address]) AS incoming on (target.[id]=incoming.[id]) when matched then update set " +
        "[name]=incoming.[name],[salary]=incoming.[salary],[address]=incoming" +
        ".[address] when not matched then insert " +
        "([name], [salary], [address], [id]) values (incoming.[name],incoming" +
        ".[salary],incoming.[address],incoming.[id]);",
        dialect.buildUpsertQueryStatement(customer, columns(customer, "id"),
                                          columns(customer, "name", "salary", "address")));
  }

  @Test
  public void upsert2() {
    TableId book = new TableId(null, null, "Book");
    assertEquals(
        "merge into [Book] with (HOLDLOCK) AS target using (select ? AS [author], ? AS [title], ?" +
        " AS [ISBN], ? AS [year], ? AS [pages])" +
        " AS incoming on (target.[author]=incoming.[author] and target.[title]=incoming.[title])" +
        " when matched then update set [ISBN]=incoming.[ISBN],[year]=incoming.[year]," +
        "[pages]=incoming.[pages] when not " +
        "matched then insert ([ISBN], [year], [pages], [author], [title]) values (incoming" +
        ".[ISBN],incoming.[year]," + "incoming.[pages],incoming.[author],incoming.[title]);",
        dialect.buildUpsertQueryStatement(book, columns(book, "author", "title"),
                                          columns(book, "ISBN", "year", "pages")));
  }
}