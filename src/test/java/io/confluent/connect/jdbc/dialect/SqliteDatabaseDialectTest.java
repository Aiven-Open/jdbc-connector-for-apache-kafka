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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import io.confluent.connect.jdbc.sink.SqliteHelper;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;

import static org.junit.Assert.assertEquals;

public class SqliteDatabaseDialectTest extends BaseDialectTest<SqliteDatabaseDialect> {

  private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());

  @Before
  public void beforeEach() throws Exception {
    sqliteHelper.setUp();
  }

  @After
  public void afterEach() throws Exception {
    sqliteHelper.tearDown();
  }

  @Override
  protected SqliteDatabaseDialect createDialect() {
    return new SqliteDatabaseDialect(sourceConfigWithUrl("jdbc:sqlite://something"));
  }


  @Test
  public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
    assertPrimitiveMapping(Type.INT8, "INTEGER");
    assertPrimitiveMapping(Type.INT16, "INTEGER");
    assertPrimitiveMapping(Type.INT32, "INTEGER");
    assertPrimitiveMapping(Type.INT64, "INTEGER");
    assertPrimitiveMapping(Type.FLOAT32, "REAL");
    assertPrimitiveMapping(Type.FLOAT64, "REAL");
    assertPrimitiveMapping(Type.BOOLEAN, "INTEGER");
    assertPrimitiveMapping(Type.BYTES, "BLOB");
    assertPrimitiveMapping(Type.STRING, "TEXT");
  }

  @Test
  public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
    assertDecimalMapping(0, "NUMERIC");
    assertDecimalMapping(3, "NUMERIC");
    assertDecimalMapping(4, "NUMERIC");
    assertDecimalMapping(5, "NUMERIC");
  }

  @Test
  public void shouldMapDataTypes() {
    verifyDataTypeMapping("INTEGER", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("REAL", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("REAL", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("TEXT", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("BLOB", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("NUMERIC", Decimal.schema(0));
    verifyDataTypeMapping("NUMERIC", Date.SCHEMA);
    verifyDataTypeMapping("NUMERIC", Time.SCHEMA);
    verifyDataTypeMapping("NUMERIC", Timestamp.SCHEMA);
  }

  @Test
  public void shouldMapDateSchemaTypeToDateSqlType() {
    assertDateMapping("NUMERIC");
  }

  @Test
  public void shouldMapTimeSchemaTypeToTimeSqlType() {
    assertTimeMapping("NUMERIC");
  }

  @Test
  public void shouldMapTimestampSchemaTypeToTimestampSqlType() {
    assertTimestampMapping("NUMERIC");
  }

  @Test
  public void shouldBuildCreateQueryStatement() {
    String expected =
        "CREATE TABLE `myTable` (\n" + "`c1` INTEGER NOT NULL,\n" + "`c2` INTEGER NOT NULL,\n" +
        "`c3` TEXT NOT NULL,\n" + "`c4` TEXT NULL,\n" + "`c5` NUMERIC DEFAULT '2001-03-15',\n" +
        "`c6` NUMERIC DEFAULT '00:00:00.000',\n" +
        "`c7` NUMERIC DEFAULT '2001-03-15 00:00:00.000',\n" + "`c8` NUMERIC NULL,\n" +
        "PRIMARY KEY(`c1`))";
    String sql = dialect.buildCreateTableStatement(tableId, sinkRecordFields);
    assertEquals(expected, sql);
  }

  @Test
  public void shouldBuildAlterTableStatement() {
    List<String> statements = dialect.buildAlterTable(tableId, sinkRecordFields);
    String[] sql = {"ALTER TABLE `myTable` ADD `c1` INTEGER NOT NULL",
                    "ALTER TABLE `myTable` ADD `c2` INTEGER NOT NULL",
                    "ALTER TABLE `myTable` ADD `c3` TEXT NOT NULL",
                    "ALTER TABLE `myTable` ADD `c4` TEXT NULL",
                    "ALTER TABLE `myTable` ADD `c5` NUMERIC DEFAULT '2001-03-15'",
                    "ALTER TABLE `myTable` ADD `c6` NUMERIC DEFAULT '00:00:00.000'",
                    "ALTER TABLE `myTable` ADD `c7` NUMERIC DEFAULT '2001-03-15 00:00:00.000'",
                    "ALTER TABLE `myTable` ADD `c8` NUMERIC NULL"};
    assertStatements(sql, statements);
  }

  @Test
  public void shouldBuildUpsertStatement() {
    String expected = "INSERT OR REPLACE INTO `myTable`(`id1`,`id2`,`columnA`,`columnB`," +
                      "`columnC`,`columnD`) VALUES(?,?,?,?,?,?)";
    String sql = dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
    assertEquals(expected, sql);
  }

  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE `myTable` (" + System.lineSeparator() + "`col1` INTEGER NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE `myTable` (" + System.lineSeparator() + "`pk1` INTEGER NOT NULL," +
        System.lineSeparator() + "PRIMARY KEY(`pk1`))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE `myTable` (" + System.lineSeparator() + "`pk1` INTEGER NOT NULL," +
        System.lineSeparator() + "`pk2` INTEGER NOT NULL," + System.lineSeparator() +
        "`col1` INTEGER NOT NULL," + System.lineSeparator() + "PRIMARY KEY(`pk1`,`pk2`))");
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol("ALTER TABLE `myTable` ADD `newcol1` INTEGER NULL");
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols("ALTER TABLE `myTable` ADD `newcol1` INTEGER NULL",
                          "ALTER TABLE `myTable` ADD `newcol2` INTEGER DEFAULT 42");
  }

  @Test
  public void upsert() {
    TableId book = new TableId(null, null, "Book");
    assertEquals(
        "INSERT OR REPLACE INTO `Book`(`author`,`title`,`ISBN`,`year`,`pages`) VALUES(?,?,?,?,?)",
        dialect.buildUpsertQueryStatement(book, columns(book, "author", "title"),
                                          columns(book, "ISBN", "year", "pages")));
  }

  @Test(expected = SQLException.class)
  public void tableOnEmptyDb() throws SQLException {
    TableId tableId = new TableId(null, null, "x");
    dialect.describeTable(sqliteHelper.connection, tableId);
  }

  @Test
  public void testDescribeTable() throws SQLException {
    TableId tableId = new TableId(null, null, "x");
    sqliteHelper.createTable(
        "create table x (id int primary key, name text not null, optional_age int null)");
    TableDefinition defn = dialect.describeTable(sqliteHelper.connection, tableId);
    assertEquals(tableId, defn.id());
    ColumnDefinition columnDefn = defn.definitionForColumn("id");
    assertEquals("INT", columnDefn.typeName());
    assertEquals(Types.INTEGER, columnDefn.type());
    assertEquals(true, columnDefn.isPrimaryKey());
    assertEquals(false, columnDefn.isOptional());

    columnDefn = defn.definitionForColumn("name");
    assertEquals("TEXT", columnDefn.typeName());
    assertEquals(Types.VARCHAR, columnDefn.type());
    assertEquals(false, columnDefn.isPrimaryKey());
    assertEquals(false, columnDefn.isOptional());

    columnDefn = defn.definitionForColumn("optional_age");
    assertEquals("INT", columnDefn.typeName());
    assertEquals(Types.INTEGER, columnDefn.type());
    assertEquals(false, columnDefn.isPrimaryKey());
    assertEquals(true, columnDefn.isOptional());
  }
}