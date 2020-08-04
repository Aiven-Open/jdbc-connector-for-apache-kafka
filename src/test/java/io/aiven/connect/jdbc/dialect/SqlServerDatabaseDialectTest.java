/*
 * Copyright 2019 Aiven Oy
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

import java.util.Arrays;
import java.util.List;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import io.aiven.connect.jdbc.sink.metadata.SinkRecordField;
import io.aiven.connect.jdbc.util.TableId;

import org.junit.Test;

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
    public void createOneColOneVarcharPk() {
        final String expected = readQueryResourceForThisTest("create_table_one_col_one_varchar_pk");
        assertQueryEquals(expected,
                dialect.buildCreateTableStatement(
                        tableId,
                        Arrays.asList(new SinkRecordField(Schema.STRING_SCHEMA, "pk1", true))
                )
        );
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
    public void shouldSanitizeUrlWithoutCredentialsInProperties() {
        assertSanitizedUrl(
            "jdbc:sqlserver://;servername=server_name;"
                + "integratedSecurity=true;authenticationScheme=JavaKerberos",
            "jdbc:sqlserver://;servername=server_name;"
                + "integratedSecurity=true;authenticationScheme=JavaKerberos"
        );
    }

    @Test
    public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
        assertSanitizedUrl(
            "jdbc:sqlserver://;servername=server_name;password=secret;keyStoreSecret=secret;"
                + "gsscredential=secret;integratedSecurity=true;authenticationScheme=JavaKerberos",
            "jdbc:sqlserver://;servername=server_name;password=****;keyStoreSecret=****;"
                + "gsscredential=****;integratedSecurity=true;authenticationScheme=JavaKerberos"
        );
        assertSanitizedUrl(
            "jdbc:sqlserver://;password=secret;servername=server_name;keyStoreSecret=secret;"
                + "gsscredential=secret;integratedSecurity=true;authenticationScheme=JavaKerberos",
            "jdbc:sqlserver://;password=****;servername=server_name;keyStoreSecret=****;"
                + "gsscredential=****;integratedSecurity=true;authenticationScheme=JavaKerberos"
        );
    }
}
