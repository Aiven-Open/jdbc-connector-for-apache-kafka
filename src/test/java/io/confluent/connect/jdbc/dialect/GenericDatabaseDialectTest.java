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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.confluent.connect.jdbc.sink.SqliteHelper;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.source.EmbeddedDerby;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ConnectionProvider;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.StringUtils;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class GenericDatabaseDialectTest extends BaseDialectTest<GenericDatabaseDialect> {

  public static final Set<String> TABLE_TYPES = Collections.singleton("TABLE");

  private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());
  private Map<String, String> connProps;
  private JdbcSourceConnectorConfig config;
  private EmbeddedDerby db;
  private ConnectionProvider connectionProvider;
  private Connection conn;

  @Before
  public void setup() throws Exception {
    db = new EmbeddedDerby();
    connProps = new HashMap<>();
    connProps.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, db.getUrl());
    connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    connProps.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
    newDialectFor(null, null);
    super.setup();
    connectionProvider = dialect;
    conn = connectionProvider.getConnection();
  }

  @After
  public void cleanup() throws Exception {
    connectionProvider.close();
    conn.close();
    db.close();
    db.dropDatabase();
  }

  @Override
  protected GenericDatabaseDialect createDialect() {
    return new GenericDatabaseDialect(sourceConfigWithUrl(db.getUrl()));
  }

  protected GenericDatabaseDialect createDialect(AbstractConfig config) {
    return new GenericDatabaseDialect(config);
  }

  protected GenericDatabaseDialect newDialectFor(
      Set<String> tableTypes,
      String schemaPattern
  ) {
    if (schemaPattern != null) {
      connProps.put(JdbcSourceConnectorConfig.SCHEMA_PATTERN_CONFIG, schemaPattern);
    } else {
      connProps.remove(JdbcSourceConnectorConfig.SCHEMA_PATTERN_CONFIG);
    }
    if (tableTypes != null) {
      connProps.put(JdbcSourceConnectorConfig.TABLE_TYPE_CONFIG, StringUtils.join(tableTypes, ","));
    } else {
      connProps.remove(JdbcSourceConnectorConfig.TABLE_TYPE_CONFIG);
    }
    config = new JdbcSourceConnectorConfig(connProps);
    dialect = createDialect(config);
    return dialect;
  }

  @Test
  public void testGetTablesEmpty() throws Exception {
    newDialectFor(TABLE_TYPES, null);
    assertEquals(Collections.emptyList(), dialect.tableIds(conn));
  }

  @Test
  public void testGetTablesSingle() throws Exception {
    newDialectFor(TABLE_TYPES, null);
    db.createTable("test", "id", "INT");
    TableId test = new TableId(null, "APP", "test");
    assertEquals(Arrays.asList(test), dialect.tableIds(conn));
  }

  @Test
  public void testFindTablesWithKnownTableType() throws Exception {
    Set<String> types = Collections.singleton("TABLE");
    newDialectFor(types, null);
    db.createTable("test", "id", "INT");
    TableId test = new TableId(null, "APP", "test");
    assertEquals(Arrays.asList(test), dialect.tableIds(conn));
  }

  @Test
  public void testNotFindTablesWithUnknownTableType() throws Exception {
    newDialectFor(Collections.singleton("view"), null);
    db.createTable("test", "id", "INT");
    assertEquals(Arrays.asList(), dialect.tableIds(conn));
  }

  @Test
  public void testGetTablesMany() throws Exception {
    newDialectFor(TABLE_TYPES, null);
    db.createTable("test", "id", "INT");
    db.createTable("foo", "id", "INT", "bar", "VARCHAR(20)");
    db.createTable("zab", "id", "INT");
    TableId test = new TableId(null, "APP", "test");
    TableId foo = new TableId(null, "APP", "foo");
    TableId zab = new TableId(null, "APP", "zab");
    assertEquals(new HashSet<>(Arrays.asList(test, foo, zab)),
                 new HashSet<>(dialect.tableIds(conn)));
  }

  @Test
  public void testGetTablesNarrowedToSchemas() throws Exception {
    db.createTable("some_table", "id", "INT");

    db.execute("CREATE SCHEMA PUBLIC_SCHEMA");
    db.execute("SET SCHEMA PUBLIC_SCHEMA");
    db.createTable("public_table", "id", "INT");

    db.execute("CREATE SCHEMA PRIVATE_SCHEMA");
    db.execute("SET SCHEMA PRIVATE_SCHEMA");
    db.createTable("private_table", "id", "INT");
    db.createTable("another_private_table", "id", "INT");

    TableId someTable = new TableId(null, "APP", "some_table");
    TableId publicTable = new TableId(null, "PUBLIC_SCHEMA", "public_table");
    TableId privateTable = new TableId(null, "PRIVATE_SCHEMA", "private_table");
    TableId anotherPrivateTable = new TableId(null, "PRIVATE_SCHEMA", "another_private_table");

    assertTableNames(TABLE_TYPES, "PUBLIC_SCHEMA", publicTable);
    assertTableNames(TABLE_TYPES, "PRIVATE_SCHEMA", privateTable, anotherPrivateTable);
    assertTableNames(TABLE_TYPES, null, someTable, publicTable, privateTable, anotherPrivateTable);
    Set<String> types = Collections.singleton("TABLE");
    assertTableNames(types, "PUBLIC_SCHEMA", publicTable);
    assertTableNames(types, "PRIVATE_SCHEMA", privateTable, anotherPrivateTable);
    assertTableNames(types, null, someTable, publicTable, privateTable, anotherPrivateTable);

    TableDefinition defn = dialect.describeTable(db.getConnection(), someTable);
    assertEquals(someTable, defn.id());
    assertEquals("INTEGER", defn.definitionForColumn("id").typeName());

    defn = dialect.describeTable(db.getConnection(), publicTable);
    assertEquals(publicTable, defn.id());
    assertEquals("INTEGER", defn.definitionForColumn("id").typeName());

    defn = dialect.describeTable(db.getConnection(), privateTable);
    assertEquals(privateTable, defn.id());
    assertEquals("INTEGER", defn.definitionForColumn("id").typeName());

    defn = dialect.describeTable(db.getConnection(), anotherPrivateTable);
    assertEquals(anotherPrivateTable, defn.id());
    assertEquals("INTEGER", defn.definitionForColumn("id").typeName());
  }

  protected void assertTableNames(
      Set<String> tableTypes,
      String schemaPattern,
      TableId... expectedTableIds
  ) throws Exception {
    newDialectFor(tableTypes, schemaPattern);
    Collection<TableId> ids = dialect.tableIds(db.getConnection());
    for (TableId expectedTableId : expectedTableIds) {
      assertTrue(ids.contains(expectedTableId));
    }
    assertEquals(expectedTableIds.length, ids.size());
  }

  @Test
  public void testDescribeTableOnEmptyDb() throws SQLException {
    TableId someTable = new TableId(null, "APP", "some_table");
    TableDefinition defn = dialect.describeTable(db.getConnection(), someTable);
    assertNull(defn);
  }

  @Test
  public void testDescribeTable() throws SQLException {
    TableId tableId = new TableId(null, "APP", "x");
    db.createTable("x",
                   "id", "INTEGER PRIMARY KEY",
                   "name", "VARCHAR(255) not null",
                   "optional_age", "INTEGER");
    TableDefinition defn = dialect.describeTable(db.getConnection(), tableId);
    assertEquals(tableId, defn.id());
    ColumnDefinition columnDefn = defn.definitionForColumn("id");
    assertEquals("INTEGER", columnDefn.typeName());
    assertEquals(Types.INTEGER, columnDefn.type());
    assertEquals(true, columnDefn.isPrimaryKey());
    assertEquals(false, columnDefn.isOptional());

    columnDefn = defn.definitionForColumn("name");
    assertEquals("VARCHAR", columnDefn.typeName());
    assertEquals(Types.VARCHAR, columnDefn.type());
    assertEquals(false, columnDefn.isPrimaryKey());
    assertEquals(false, columnDefn.isOptional());

    columnDefn = defn.definitionForColumn("optional_age");
    assertEquals("INTEGER", columnDefn.typeName());
    assertEquals(Types.INTEGER, columnDefn.type());
    assertEquals(false, columnDefn.isPrimaryKey());
    assertEquals(true, columnDefn.isOptional());
  }

  @Test
  public void testDescribeColumns() throws Exception {
    // Normal case
    db.createTable("test", "id", "INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY", "bar", "INTEGER");
    TableId test = new TableId(null, "APP", "test");
    ColumnId id = new ColumnId(test, "id");
    ColumnId bar = new ColumnId(test, "bar");
    Map<ColumnId, ColumnDefinition> defns = dialect
        .describeColumns(db.getConnection(), "test", null);
    System.out.println("defns = " + defns);
    assertTrue(defns.get(id).isAutoIncrement());
    assertFalse(defns.get(bar).isAutoIncrement());
    assertFalse(defns.get(id).isOptional());
    assertTrue(defns.get(bar).isOptional());

    // No auto increment
    db.createTable("none", "id", "INTEGER", "bar", "INTEGER");
    TableId none = new TableId(null, "APP", "none");
    id = new ColumnId(none, "id");
    bar = new ColumnId(none, "bar");
    defns = dialect.describeColumns(db.getConnection(), "none", null);
    assertFalse(defns.get(id).isAutoIncrement());
    assertFalse(defns.get(bar).isAutoIncrement());
    assertTrue(defns.get(id).isOptional());
    assertTrue(defns.get(bar).isOptional());

    // We can't check multiple columns because Derby ties auto increment to identity and
    // disallows multiple auto increment columns. This is probably ok, multiple auto increment
    // columns should be very unusual anyway

    // Normal case, mixed in and not the first column
    db.createTable("mixed", "foo", "INTEGER", "id", "INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY",
                   "bar", "INTEGER");
    TableId mixed = new TableId(null, "APP", "mixed");
    ColumnId foo = new ColumnId(mixed, "foo");
    id = new ColumnId(mixed, "id");
    bar = new ColumnId(mixed, "bar");
    defns = dialect.describeColumns(db.getConnection(), "mixed", null);
    assertFalse(defns.get(foo).isAutoIncrement());
    assertTrue(defns.get(id).isAutoIncrement());
    assertFalse(defns.get(bar).isAutoIncrement());

    // Derby does not seem to allow null
    db.createTable("tstest", "ts", "TIMESTAMP NOT NULL", "tsdefault", "TIMESTAMP", "tsnull",
                   "TIMESTAMP DEFAULT NULL");
    TableId tstest = new TableId(null, "APP", "tstest");
    ColumnId ts = new ColumnId(tstest, "ts");
    ColumnId tsdefault = new ColumnId(tstest, "tsdefault");
    ColumnId tsnull = new ColumnId(tstest, "tsnull");

    defns = dialect.describeColumns(db.getConnection(), "tstest", null);
    assertFalse(defns.get(ts).isOptional());
    // The default for TIMESTAMP columns can vary between databases, but for Derby it is nullable
    assertTrue(defns.get(tsdefault).isOptional());
    assertTrue(defns.get(tsnull).isOptional());
  }

  @Test(expected = ConnectException.class)
  public void shouldBuildCreateQueryStatement() {
    dialect.buildCreateTableStatement(tableId, sinkRecordFields);
  }

  @Test(expected = ConnectException.class)
  public void shouldBuildAlterTableStatement() {
    dialect.buildAlterTable(tableId, sinkRecordFields);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void shouldBuildUpsertStatement() {
    dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
  }


  @Test
  public void formatColumnValue() {
    verifyFormatColumnValue("42", Schema.INT8_SCHEMA, (byte) 42);
    verifyFormatColumnValue("42", Schema.INT16_SCHEMA, (short) 42);
    verifyFormatColumnValue("42", Schema.INT32_SCHEMA, 42);
    verifyFormatColumnValue("42", Schema.INT64_SCHEMA, 42L);
    verifyFormatColumnValue("42.5", Schema.FLOAT32_SCHEMA, 42.5f);
    verifyFormatColumnValue("42.5", Schema.FLOAT64_SCHEMA, 42.5d);
    verifyFormatColumnValue("0", Schema.BOOLEAN_SCHEMA, false);
    verifyFormatColumnValue("1", Schema.BOOLEAN_SCHEMA, true);
    verifyFormatColumnValue("'quoteit'", Schema.STRING_SCHEMA, "quoteit");
    verifyFormatColumnValue("x'2A'", Schema.BYTES_SCHEMA, new byte[]{42});

    verifyFormatColumnValue("42.42", Decimal.schema(2), new BigDecimal("42.42"));

    final java.util.Date instant = new java.util.Date(1474661402123L);
    verifyFormatColumnValue("'2016-09-23'", Date.SCHEMA, instant);
    verifyFormatColumnValue("'20:10:02.123'", Time.SCHEMA, instant);
    verifyFormatColumnValue("'2016-09-23 20:10:02.123'", Timestamp.SCHEMA, instant);
  }

  private void verifyFormatColumnValue(String expected, Schema schema, Object value) {
    GenericDatabaseDialect dialect = dummyDialect();
    ExpressionBuilder builder = dialect.expressionBuilder();
    dialect.formatColumnValue(builder, schema.name(), schema.parameters(), schema.type(), value);
    assertEquals(expected, builder.toString());
  }

  private void verifyWriteColumnSpec(String expected, SinkRecordField field) {
    GenericDatabaseDialect dialect = dummyDialect();
    ExpressionBuilder builder = dialect.expressionBuilder();
    dialect.writeColumnSpec(builder, field);
    assertEquals(expected, builder.toString());
  }

  private GenericDatabaseDialect dummyDialect() {
    IdentifierRules rules = new IdentifierRules(",", "`", "`");
    return new GenericDatabaseDialect(config, rules) {
      @Override
      protected String getSqlType(SinkRecordField f) {
        return "DUMMY";
      }
    };
  }

  @Test
  public void writeColumnSpec() {
    verifyWriteColumnSpec("\"foo\" DUMMY DEFAULT 42", new SinkRecordField(
        SchemaBuilder.int32().defaultValue(42).build(), "foo", true));
    verifyWriteColumnSpec("\"foo\" DUMMY DEFAULT 42", new SinkRecordField(SchemaBuilder.int32().defaultValue(42).build(), "foo", false));
    verifyWriteColumnSpec("\"foo\" DUMMY DEFAULT 42", new SinkRecordField(SchemaBuilder.int32().optional().defaultValue(42).build(), "foo", true));
    verifyWriteColumnSpec("\"foo\" DUMMY DEFAULT 42", new SinkRecordField(SchemaBuilder.int32().optional().defaultValue(42).build(), "foo", false));
    verifyWriteColumnSpec("\"foo\" DUMMY NOT NULL", new SinkRecordField(Schema.INT32_SCHEMA, "foo", true));
    verifyWriteColumnSpec("\"foo\" DUMMY NOT NULL", new SinkRecordField(Schema.INT32_SCHEMA, "foo", false));
    verifyWriteColumnSpec("\"foo\" DUMMY NOT NULL", new SinkRecordField(Schema.OPTIONAL_INT32_SCHEMA, "foo", true));
    verifyWriteColumnSpec("\"foo\" DUMMY NULL", new SinkRecordField(Schema.OPTIONAL_INT32_SCHEMA, "foo", false));
  }
}