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

import org.junit.Test;

public class VerticaDatabaseDialectTest extends BaseDialectTest<VerticaDatabaseDialect> {

    @Override
    protected VerticaDatabaseDialect createDialect() {
        return new VerticaDatabaseDialect(sourceConfigWithUrl("jdbc:vertica://something"));
    }

    @Test
    public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
        assertPrimitiveMapping(Type.INT8, "INT");
        assertPrimitiveMapping(Type.INT16, "INT");
        assertPrimitiveMapping(Type.INT32, "INT");
        assertPrimitiveMapping(Type.INT64, "INT");
        assertPrimitiveMapping(Type.FLOAT32, "FLOAT");
        assertPrimitiveMapping(Type.FLOAT64, "FLOAT");
        assertPrimitiveMapping(Type.BOOLEAN, "BOOLEAN");
        assertPrimitiveMapping(Type.BYTES, "VARBINARY(1024)");
        assertPrimitiveMapping(Type.STRING, "VARCHAR(1024)");
    }

    @Test
    public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
        assertDecimalMapping(0, "DECIMAL(18,0)");
        assertDecimalMapping(3, "DECIMAL(18,3)");
        assertDecimalMapping(4, "DECIMAL(18,4)");
        assertDecimalMapping(5, "DECIMAL(18,5)");
    }

    @Test
    public void shouldMapDataTypes() {
        verifyDataTypeMapping("INT", Schema.INT8_SCHEMA);
        verifyDataTypeMapping("INT", Schema.INT16_SCHEMA);
        verifyDataTypeMapping("INT", Schema.INT32_SCHEMA);
        verifyDataTypeMapping("INT", Schema.INT64_SCHEMA);
        verifyDataTypeMapping("FLOAT", Schema.FLOAT32_SCHEMA);
        verifyDataTypeMapping("FLOAT", Schema.FLOAT64_SCHEMA);
        verifyDataTypeMapping("BOOLEAN", Schema.BOOLEAN_SCHEMA);
        verifyDataTypeMapping("VARCHAR(1024)", Schema.STRING_SCHEMA);
        verifyDataTypeMapping("VARBINARY(1024)", Schema.BYTES_SCHEMA);
        verifyDataTypeMapping("DECIMAL(18,0)", Decimal.schema(0));
        verifyDataTypeMapping("DECIMAL(18,4)", Decimal.schema(4));
        verifyDataTypeMapping("DATE", Date.SCHEMA);
        verifyDataTypeMapping("TIME", Time.SCHEMA);
        verifyDataTypeMapping("TIMESTAMP", Timestamp.SCHEMA);
    }

    @Test
    public void shouldMapDateSchemaTypeToDateSqlType() {
        assertDateMapping("DATE");
    }

    @Test
    public void shouldMapTimeSchemaTypeToTimeSqlType() {
        assertTimeMapping("TIME");
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
        final String[] expected = readQueryResourceLinesForThisTest("alter_table");
        final List<String> actual = dialect.buildAlterTable(tableId, sinkRecordFields);
        assertStatements(expected, actual);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldBuildUpsertStatement() {
        dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
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
        final String[] expected = readQueryResourceLinesForThisTest("alter_add_one_col");
        verifyAlterAddOneCol(expected);
    }

    @Test
    public void alterAddTwoCol() {
        final String[] expected = readQueryResourceLinesForThisTest("alter_add_two_cols");
        verifyAlterAddTwoCols(expected);
    }

    @Test
    public void shouldSanitizeUrlWithoutCredentialsInProperties() {
        assertSanitizedUrl(
            "jdbc:vertica://something?key1=value1&key2=value2&key3=value3&&other=value",
            "jdbc:vertica://something?key1=value1&key2=value2&key3=value3&&other=value"

        );
    }

    @Test
    public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
        assertSanitizedUrl(
            "jdbc:vertica://something?password=secret&key1=value1&key2=value2&key3=value3&"
                + "user=smith&password=secret&other=value",
            "jdbc:vertica://something?password=****&key1=value1&key2=value2&key3=value3&"
                + "user=smith&password=****&other=value"
        );
    }
}
