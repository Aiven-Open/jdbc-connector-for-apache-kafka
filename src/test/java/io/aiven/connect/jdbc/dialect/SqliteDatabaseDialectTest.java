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

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import io.aiven.connect.jdbc.sink.SqliteHelper;
import io.aiven.connect.jdbc.util.ColumnDefinition;
import io.aiven.connect.jdbc.util.TableDefinition;
import io.aiven.connect.jdbc.util.TableId;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SqliteDatabaseDialectTest extends BaseDialectTest<SqliteDatabaseDialect> {

    private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

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
        try {
            final String dbFile = File.createTempFile("sqlite", "", folder.getRoot()).toString();
            return new SqliteDatabaseDialect(sourceConfigWithUrl("jdbc:sqlite:" + dbFile));
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
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
        final String expected = readQueryResourceForThisTest("create_table");
        final String sql = dialect.buildCreateTableStatement(tableId, sinkRecordFields);
        assertQueryEquals(expected, sql);
    }

    @Test
    public void shouldBuildAlterTableStatement() {
        final String[] expected = readQueryResourceLinesForThisTest("alter_table");
        final List<String> actual = dialect.buildAlterTable(tableId, sinkRecordFields);
        assertStatements(expected, actual);
    }

    @Test
    public void shouldBuildUpsertStatement() {
        final String expected = readQueryResourceForThisTest("upsert0");
        final String actual = dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
        assertQueryEquals(expected, actual);
    }

    @Test
    public void createOneColNoPk() {
        final String expected = readQueryResourceForThisTest("create_table_one_col_no_pk");
        verifyCreateOneColNoPk(expected);
    }

    @Test
    public void createOneColOnePk() {
        final String expected = readQueryResourceForThisTest("create_table_one_col_one_pk");
        verifyCreateOneColOnePk(expected);
    }

    @Test
    public void createThreeColTwoPk() {
        final String expected = readQueryResourceForThisTest("create_table_three_cols_two_pks");
        verifyCreateThreeColTwoPk(expected);
    }

    @Test
    public void alterAddOneCol() {
        final String expected = readQueryResourceForThisTest("alter_add_one_col");
        verifyAlterAddOneCol(expected);
    }

    @Test
    public void alterAddTwoCol() {
        final String[] expected = readQueryResourceLinesForThisTest("alter_add_two_cols");
        verifyAlterAddTwoCols(expected);
    }

    @Test
    public void upsert() {
        final String expected = readQueryResourceForThisTest("upsert1");
        final TableId book = new TableId(null, null, "Book");
        final String actual = dialect.buildUpsertQueryStatement(
            book,
            columns(book, "author", "title"),
            columns(book, "ISBN", "year", "pages")
        );
        assertQueryEquals(expected, actual);
    }

    @Test(expected = SQLException.class)
    public void tableOnEmptyDb() throws SQLException {
        final TableId tableId = new TableId(null, null, "x");
        dialect.describeTable(sqliteHelper.connection, tableId);
    }

    @Test
    public void testDescribeTable() throws SQLException {
        final TableId tableId = new TableId(null, null, "x");
        sqliteHelper.createTable(
            "create table x (id int primary key, name text not null, optional_age int null)");
        final TableDefinition defn = dialect.describeTable(sqliteHelper.connection, tableId);
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

    @Test
    public void testGettingCurrentTimeOnDB() throws SQLException {
        final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC));
        final Instant dbInstant = dialect.currentTimeOnDB(sqliteHelper.connection, cal).toInstant();
        // Check that the UTC timezone is correct.
        final long diffSec = Math.abs(Duration.between(dbInstant, Instant.now()).getSeconds());
        assertTrue(diffSec <= 3);
    }
}
