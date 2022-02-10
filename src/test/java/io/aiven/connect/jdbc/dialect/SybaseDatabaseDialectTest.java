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
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import io.aiven.connect.jdbc.util.DateTimeUtils;
import io.aiven.connect.jdbc.util.TableId;

import org.junit.Test;

public class SybaseDatabaseDialectTest extends BaseDialectTest<SybaseDatabaseDialect> {

    @Override
    protected SybaseDatabaseDialect createDialect() {
        return new SybaseDatabaseDialect(sourceConfigWithUrl("jdbc:jtds:sybase://something"));
    }

    @Test
    public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
        assertPrimitiveMapping(Type.INT8, "smallint");
        assertPrimitiveMapping(Type.INT16, "smallint");
        assertPrimitiveMapping(Type.INT32, "int");
        assertPrimitiveMapping(Type.INT64, "bigint");
        assertPrimitiveMapping(Type.FLOAT32, "real");
        assertPrimitiveMapping(Type.FLOAT64, "float");
        assertPrimitiveMapping(Type.BOOLEAN, "bit");
        assertPrimitiveMapping(Type.BYTES, "image");
        assertPrimitiveMapping(Type.STRING, "text");
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
        verifyDataTypeMapping("smallint", Schema.INT8_SCHEMA);
        verifyDataTypeMapping("smallint", Schema.INT16_SCHEMA);
        verifyDataTypeMapping("int", Schema.INT32_SCHEMA);
        verifyDataTypeMapping("bigint", Schema.INT64_SCHEMA);
        verifyDataTypeMapping("real", Schema.FLOAT32_SCHEMA);
        verifyDataTypeMapping("float", Schema.FLOAT64_SCHEMA);
        verifyDataTypeMapping("bit", Schema.BOOLEAN_SCHEMA);
        verifyDataTypeMapping("text", Schema.STRING_SCHEMA);
        verifyDataTypeMapping("image", Schema.BYTES_SCHEMA);
        verifyDataTypeMapping("decimal(38,0)", Decimal.schema(0));
        verifyDataTypeMapping("decimal(38,4)", Decimal.schema(4));
        verifyDataTypeMapping("date", Date.SCHEMA);
        verifyDataTypeMapping("time", Time.SCHEMA);
        verifyDataTypeMapping("datetime", Timestamp.SCHEMA);
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
        assertTimestampMapping("datetime");
    }

    @Test
    public void shouldBuildCreateTableStatement() {
        final String expected = readQueryResourceForThisTest("create_table");
        final String actual = dialect.buildCreateTableStatement(tableId, sinkRecordFields);
        assertQueryEquals(expected, actual);
    }

    @Test
    public void shouldBuildDropTableStatement() {
        final String expected = readQueryResourceForThisTest("drop_table");
        final String actual = dialect.buildDropTableStatement(tableId, new DropOptions().setIfExists(false));
        assertQueryEquals(expected, actual);
    }

    @Test
    public void shouldBuildDropTableStatementWithIfExistsClause() {
        final String expected = readQueryResourceForThisTest("drop_table_if_exists");
        final String actual = dialect.buildDropTableStatement(tableId, new DropOptions().setIfExists(true));
        assertQueryEquals(expected, actual);
    }

    @Test
    public void shouldBuildDropTableStatementWithIfExistsClauseAndSchemaNameInTableId() {
        final String expected = readQueryResourceForThisTest("drop_table_with_schema_if_exists");
        tableId = new TableId("dbName", "dbo", "myTable");
        final String actual = dialect.buildDropTableStatement(tableId, new DropOptions().setIfExists(true));
        assertQueryEquals(expected, actual);
    }

    @Test
    public void shouldBuildAlterTableStatement() {
        final String[] expected = {
            readQueryResourceForThisTest("alter_table")
        };
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
        final String expected = readQueryResourceForThisTest("alter_add_two_cols");
        verifyAlterAddTwoCols(expected);
    }

    @Test
    public void upsert1() {
        final String expected = readQueryResourceForThisTest("upsert1");
        final TableId customer = tableId("Customer");
        final String actual = dialect.buildUpsertQueryStatement(
            customer,
            columns(customer, "id"),
            columns(customer, "name", "salary", "address")
        );
        assertQueryEquals(expected, actual);
    }

    @Test
    public void upsert2() {
        final String expected = readQueryResourceForThisTest("upsert2");
        final TableId book = new TableId(null, null, "Book");
        final String actual = dialect.buildUpsertQueryStatement(
            book,
            columns(book, "author", "title"),
            columns(book, "ISBN", "year", "pages")
        );
        assertQueryEquals(expected, actual);
    }

    @Test
    public void bindFieldPrimitiveValues() throws SQLException {
        int index = ThreadLocalRandom.current().nextInt();
        verifyBindField(++index, Schema.INT8_SCHEMA, (short) 42).setShort(index, (short) 42);
        verifyBindField(++index, Schema.INT8_SCHEMA, (short) -42).setShort(index, (short) -42);
        verifyBindField(++index, Schema.INT16_SCHEMA, (short) 42).setShort(index, (short) 42);
        verifyBindField(++index, Schema.INT32_SCHEMA, 42).setInt(index, 42);
        verifyBindField(++index, Schema.INT64_SCHEMA, 42L).setLong(index, 42L);
        verifyBindField(++index, Schema.BOOLEAN_SCHEMA, false).setBoolean(index, false);
        verifyBindField(++index, Schema.BOOLEAN_SCHEMA, true).setBoolean(index, true);
        verifyBindField(++index, Schema.FLOAT32_SCHEMA, -42f).setFloat(index, -42f);
        verifyBindField(++index, Schema.FLOAT64_SCHEMA, 42d).setDouble(index, 42d);
        verifyBindField(++index, Schema.BYTES_SCHEMA, new byte[]{42}).setBytes(index, new byte[]{42});
        verifyBindField(++index,
            Schema.BYTES_SCHEMA,
            ByteBuffer.wrap(new byte[]{42})).setBytes(index, new byte[]{42});
        verifyBindField(++index, Schema.STRING_SCHEMA, "yep").setString(index, "yep");
        verifyBindField(++index,
            Decimal.schema(0),
            new BigDecimal("1.5").setScale(0, BigDecimal.ROUND_HALF_EVEN))
            .setBigDecimal(index, new BigDecimal(2));
        final Calendar utcCalendar = DateTimeUtils.getTimeZoneCalendar(TimeZone.getTimeZone(ZoneOffset.UTC));
        verifyBindField(
            ++index,
            Date.SCHEMA,
            new java.util.Date(0)
        ).setDate(index, new java.sql.Date(0), utcCalendar);
        verifyBindField(
            ++index,
            Time.SCHEMA,
            new java.util.Date(1000)
        ).setTime(index, new java.sql.Time(1000), utcCalendar);
        verifyBindField(
            ++index,
            Timestamp.SCHEMA,
            new java.util.Date(100)
        ).setTimestamp(index, new java.sql.Timestamp(100), utcCalendar);
    }

    @Test
    public void shouldSanitizeUrlWithoutCredentialsInProperties() {
        assertSanitizedUrl(
            "jdbc:jtds:sybase://something?key1=value1&key2=value2&key3=value3&&other=value",
            "jdbc:jtds:sybase://something?key1=value1&key2=value2&key3=value3&&other=value"

        );
    }

    @Test
    public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
        assertSanitizedUrl(
            "jdbc:jtds:sybase://something?password=secret&key1=value1&key2=value2&key3=value3&"
                + "user=smith&password=secret&other=value",
            "jdbc:jtds:sybase://something?password=****&key1=value1&key2=value2&key3=value3&"
                + "user=smith&password=****&other=value"
        );
    }
}
