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

import java.util.List;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import io.aiven.connect.jdbc.util.TableId;

import org.junit.Test;

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
        final String expected = readQueryResourceForThisTest("create_table");
        final String actual = dialect.buildCreateTableStatement(tableId, sinkRecordFields);
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
    public void upsert() {
        final String expected = readQueryResourceForThisTest("upsert1");
        final TableId article = tableId("ARTICLE");
        final String actual = dialect.buildUpsertQueryStatement(
            article,
            columns(article, "title", "author"),
            columns(article, "body")
        );
        assertQueryEquals(expected, actual);
    }

    @Test
    public void shouldSanitizeUrlWithCredentialsInHosts() {
        assertSanitizedUrl(
            "jdbc:oracle:thin:sandy/secret@myhost:1111/db?key1=value1",
            "jdbc:oracle:thin:sandy/****@myhost:1111/db?key1=value1"
        );
        assertSanitizedUrl(
            "jdbc:oracle:oci8:sandy/secret@host=myhost1,port=1111/db?key1=value1",
            "jdbc:oracle:oci8:sandy/****@host=myhost1,port=1111/db?key1=value1"
        );
    }

    @Test
    public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
        assertSanitizedUrl(
            "jdbc:oracle:thin:@myhost:1111/db?password=secret&key1=value1&"
                + "key2=value2&key3=value3&user=smith&password=secret&other=value",
            "jdbc:oracle:thin:@myhost:1111/db?password=****&key1=value1&"
                + "key2=value2&key3=value3&user=smith&password=****&other=value"
        );
    }
}
