/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2017 Confluent Inc.
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

package io.aiven.connect.jdbc.dialect;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.connect.jdbc.config.JdbcConfig;
import io.aiven.connect.jdbc.sink.SqliteHelper;
import io.aiven.connect.jdbc.sink.metadata.SinkRecordField;
import io.aiven.connect.jdbc.source.EmbeddedDerby;
import io.aiven.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.aiven.connect.jdbc.util.ColumnDefinition;
import io.aiven.connect.jdbc.util.ColumnId;
import io.aiven.connect.jdbc.util.ConnectionProvider;
import io.aiven.connect.jdbc.util.ExpressionBuilder;
import io.aiven.connect.jdbc.util.IdentifierRules;
import io.aiven.connect.jdbc.util.StringUtils;
import io.aiven.connect.jdbc.util.TableDefinition;
import io.aiven.connect.jdbc.util.TableId;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
        connProps.put(JdbcConfig.CONNECTION_URL_CONFIG, db.getUrl());
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

    protected GenericDatabaseDialect createDialect(final JdbcConfig config) {
        return new GenericDatabaseDialect(config);
    }

    protected GenericDatabaseDialect newDialectFor(
        final Set<String> tableTypes,
        final String schemaPattern
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
        assertThat(dialect.tableIds(conn)).isEmpty();
    }

    @Test
    public void testGetTablesSingle() throws Exception {
        newDialectFor(TABLE_TYPES, null);
        db.createTable("test", "id", "INT");
        final TableId test = new TableId(null, "APP", "test");
        assertThat(dialect.tableIds(conn)).containsExactly(test);
    }

    @Test
    public void testFindTablesWithKnownTableType() throws Exception {
        final Set<String> types = Collections.singleton("TABLE");
        newDialectFor(types, null);
        db.createTable("test", "id", "INT");
        final TableId test = new TableId(null, "APP", "test");
        assertThat(dialect.tableIds(conn)).containsExactly(test);
    }

    @Test
    public void testNotFindTablesWithUnknownTableType() throws Exception {
        newDialectFor(Collections.singleton("view"), null);
        db.createTable("test", "id", "INT");
        assertThat(dialect.tableIds(conn)).isEmpty();
    }

    @Test
    public void testGetTablesMany() throws Exception {
        newDialectFor(TABLE_TYPES, null);
        db.createTable("test", "id", "INT");
        db.createTable("foo", "id", "INT", "bar", "VARCHAR(20)");
        db.createTable("zab", "id", "INT");
        final TableId test = new TableId(null, "APP", "test");
        final TableId foo = new TableId(null, "APP", "foo");
        final TableId zab = new TableId(null, "APP", "zab");
        assertThat(dialect.tableIds(conn)).containsExactlyInAnyOrder(test, foo, zab);
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

        final TableId someTable = new TableId(null, "APP", "some_table");
        final TableId publicTable = new TableId(null, "PUBLIC_SCHEMA", "public_table");
        final TableId privateTable = new TableId(null, "PRIVATE_SCHEMA", "private_table");
        final TableId anotherPrivateTable = new TableId(null, "PRIVATE_SCHEMA", "another_private_table");

        assertTableNames(TABLE_TYPES, "PUBLIC_SCHEMA", publicTable);
        assertTableNames(TABLE_TYPES, "PRIVATE_SCHEMA", privateTable, anotherPrivateTable);
        assertTableNames(TABLE_TYPES, null, someTable, publicTable, privateTable, anotherPrivateTable);
        final Set<String> types = Collections.singleton("TABLE");
        assertTableNames(types, "PUBLIC_SCHEMA", publicTable);
        assertTableNames(types, "PRIVATE_SCHEMA", privateTable, anotherPrivateTable);
        assertTableNames(types, null, someTable, publicTable, privateTable, anotherPrivateTable);

        TableDefinition defn = dialect.describeTable(db.getConnection(), someTable);
        assertThat(defn.id()).isEqualTo(someTable);
        assertThat(defn.definitionForColumn("id").typeName()).isEqualTo("INTEGER");

        defn = dialect.describeTable(db.getConnection(), publicTable);
        assertThat(defn.id()).isEqualTo(publicTable);
        assertThat(defn.definitionForColumn("id").typeName()).isEqualTo("INTEGER");

        defn = dialect.describeTable(db.getConnection(), privateTable);
        assertThat(defn.id()).isEqualTo(privateTable);
        assertThat(defn.definitionForColumn("id").typeName()).isEqualTo("INTEGER");

        defn = dialect.describeTable(db.getConnection(), anotherPrivateTable);
        assertThat(defn.id()).isEqualTo(anotherPrivateTable);
        assertThat(defn.definitionForColumn("id").typeName()).isEqualTo("INTEGER");
    }

    protected void assertTableNames(
        final Set<String> tableTypes,
        final String schemaPattern,
        final TableId... expectedTableIds
    ) throws Exception {
        newDialectFor(tableTypes, schemaPattern);
        final Collection<TableId> ids = dialect.tableIds(db.getConnection());
        for (final TableId expectedTableId : expectedTableIds) {
            assertThat(ids).contains(expectedTableId);
        }
        assertThat(ids).hasSameSizeAs(expectedTableIds);
    }

    @Test
    public void testDescribeTableOnEmptyDb() throws SQLException {
        final TableId someTable = new TableId(null, "APP", "some_table");
        final TableDefinition defn = dialect.describeTable(db.getConnection(), someTable);
        assertThat(defn).isNull();
    }

    @Test
    public void testDescribeTable() throws SQLException {
        final TableId tableId = new TableId(null, "APP", "x");
        db.createTable("x",
            "id", "INTEGER PRIMARY KEY",
            "name", "VARCHAR(255) not null",
            "optional_age", "INTEGER");
        final TableDefinition defn = dialect.describeTable(db.getConnection(), tableId);
        assertThat(defn.id()).isEqualTo(tableId);
        ColumnDefinition columnDefn = defn.definitionForColumn("id");
        assertThat(columnDefn.typeName()).isEqualTo("INTEGER");
        assertThat(columnDefn.type()).isEqualTo(Types.INTEGER);
        assertThat(columnDefn.isPrimaryKey()).isTrue();
        assertThat(columnDefn.isOptional()).isFalse();

        columnDefn = defn.definitionForColumn("name");
        assertThat(columnDefn.typeName()).isEqualTo("VARCHAR");
        assertThat(columnDefn.type()).isEqualTo(Types.VARCHAR);
        assertThat(columnDefn.isPrimaryKey()).isFalse();
        assertThat(columnDefn.isOptional()).isFalse();

        columnDefn = defn.definitionForColumn("optional_age");
        assertThat(columnDefn.typeName()).isEqualTo("INTEGER");
        assertThat(columnDefn.type()).isEqualTo(Types.INTEGER);
        assertThat(columnDefn.isPrimaryKey()).isFalse();
        assertThat(columnDefn.isOptional()).isTrue();
    }

    @Test
    public void testDescribeColumns() throws Exception {
        // Normal case
        db.createTable("test", "id", "INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY", "bar", "INTEGER");
        final TableId test = new TableId(null, "APP", "test");
        ColumnId id = new ColumnId(test, "id");
        ColumnId bar = new ColumnId(test, "bar");
        Map<ColumnId, ColumnDefinition> defns = dialect
            .describeColumns(db.getConnection(), "test", null);
        assertThat(defns.get(id).isAutoIncrement()).isTrue();
        assertThat(defns.get(bar).isAutoIncrement()).isFalse();
        assertThat(defns.get(id).isOptional()).isFalse();
        assertThat(defns.get(bar).isOptional()).isTrue();

        // No auto increment
        db.createTable("none", "id", "INTEGER", "bar", "INTEGER");
        final TableId none = new TableId(null, "APP", "none");
        id = new ColumnId(none, "id");
        bar = new ColumnId(none, "bar");
        defns = dialect.describeColumns(db.getConnection(), "none", null);
        assertThat(defns.get(id).isAutoIncrement()).isFalse();
        assertThat(defns.get(bar).isAutoIncrement()).isFalse();
        assertThat(defns.get(id).isOptional()).isTrue();
        assertThat(defns.get(bar).isOptional()).isTrue();

        // We can't check multiple columns because Derby ties auto increment to identity and
        // disallows multiple auto increment columns. This is probably ok, multiple auto increment
        // columns should be very unusual anyway

        // Normal case, mixed in and not the first column
        db.createTable("mixed", "foo", "INTEGER", "id", "INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY",
            "bar", "INTEGER");
        final TableId mixed = new TableId(null, "APP", "mixed");
        final ColumnId foo = new ColumnId(mixed, "foo");
        id = new ColumnId(mixed, "id");
        bar = new ColumnId(mixed, "bar");
        defns = dialect.describeColumns(db.getConnection(), "mixed", null);
        assertThat(defns.get(foo).isAutoIncrement()).isFalse();
        assertThat(defns.get(id).isAutoIncrement()).isTrue();
        assertThat(defns.get(bar).isAutoIncrement()).isFalse();

        // Derby does not seem to allow null
        db.createTable("tstest", "ts", "TIMESTAMP NOT NULL", "tsdefault", "TIMESTAMP", "tsnull",
            "TIMESTAMP DEFAULT NULL");
        final TableId tstest = new TableId(null, "APP", "tstest");
        final ColumnId ts = new ColumnId(tstest, "ts");
        final ColumnId tsdefault = new ColumnId(tstest, "tsdefault");
        final ColumnId tsnull = new ColumnId(tstest, "tsnull");

        defns = dialect.describeColumns(db.getConnection(), "tstest", null);
        assertThat(defns.get(ts).isOptional()).isFalse();
        // The default for TIMESTAMP columns can vary between databases, but for Derby it is nullable
        assertThat(defns.get(tsdefault).isOptional()).isTrue();
        assertThat(defns.get(tsnull).isOptional()).isTrue();
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

    private void verifyFormatColumnValue(final String expected, final Schema schema, final Object value) {
        final GenericDatabaseDialect dialect = dummyDialect();
        final ExpressionBuilder builder = dialect.expressionBuilder();
        dialect.formatColumnValue(builder, schema.name(), schema.parameters(), schema.type(), value);
        assertThat(builder).hasToString(expected);
    }

    private void verifyWriteColumnSpec(final String expected, final SinkRecordField field) {
        final GenericDatabaseDialect dialect = dummyDialect();
        final ExpressionBuilder builder = dialect.expressionBuilder();
        dialect.writeColumnSpec(builder, field);
        assertThat(builder).hasToString(expected);
    }

    private GenericDatabaseDialect dummyDialect() {
        final IdentifierRules rules = new IdentifierRules(",", "`", "`");
        return new GenericDatabaseDialect(config, rules) {
            @Override
            protected String getSqlType(final SinkRecordField f) {
                return "DUMMY";
            }
        };
    }

    @Test
    public void writeColumnSpec() {
        verifyWriteColumnSpec("\"foo\" DUMMY DEFAULT 42",
            new SinkRecordField(
                SchemaBuilder.int32().defaultValue(42).build(), "foo", true));
        verifyWriteColumnSpec("\"foo\" DUMMY DEFAULT 42",
            new SinkRecordField(SchemaBuilder.int32().defaultValue(42).build(), "foo", false));
        verifyWriteColumnSpec("\"foo\" DUMMY DEFAULT 42",
            new SinkRecordField(SchemaBuilder.int32().optional().defaultValue(42).build(), "foo", true));
        verifyWriteColumnSpec("\"foo\" DUMMY DEFAULT 42",
            new SinkRecordField(SchemaBuilder.int32().optional().defaultValue(42).build(), "foo", false));
        verifyWriteColumnSpec("\"foo\" DUMMY NOT NULL",
            new SinkRecordField(Schema.INT32_SCHEMA, "foo", true));
        verifyWriteColumnSpec("\"foo\" DUMMY NOT NULL",
            new SinkRecordField(Schema.INT32_SCHEMA, "foo", false));
        verifyWriteColumnSpec("\"foo\" DUMMY NOT NULL",
            new SinkRecordField(Schema.OPTIONAL_INT32_SCHEMA, "foo", true));
        verifyWriteColumnSpec("\"foo\" DUMMY NULL",
            new SinkRecordField(Schema.OPTIONAL_INT32_SCHEMA, "foo", false));
    }

    @Test
    public void shouldSanitizeUrlWithoutCredentialsInProperties() {
        assertSanitizedUrl(
            "jdbc:acme:db/foo:100?key1=value1&key2=value2&key3=value3&&other=value",
            "jdbc:acme:db/foo:100?key1=value1&key2=value2&key3=value3&&other=value"

        );
    }

    @Test
    public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
        assertSanitizedUrl(
            "jdbc:acme:db/foo:100?password=secret&key1=value1&key2=value2&key3=value3&"
                + "user=smith&password=secret&other=value",
            "jdbc:acme:db/foo:100?password=****&key1=value1&key2=value2&key3=value3&"
                + "user=smith&password=****&other=value"
        );
    }
}
