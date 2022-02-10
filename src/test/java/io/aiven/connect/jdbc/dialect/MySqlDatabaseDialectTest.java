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

public class MySqlDatabaseDialectTest extends BaseDialectTest<MySqlDatabaseDialect> {

    @Override
    protected MySqlDatabaseDialect createDialect() {
        return new MySqlDatabaseDialect(sourceConfigWithUrl("jdbc:mysql://something"));
    }

    @Test
    public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
        assertPrimitiveMapping(Type.INT8, "TINYINT");
        assertPrimitiveMapping(Type.INT16, "SMALLINT");
        assertPrimitiveMapping(Type.INT32, "INT");
        assertPrimitiveMapping(Type.INT64, "BIGINT");
        assertPrimitiveMapping(Type.FLOAT32, "FLOAT");
        assertPrimitiveMapping(Type.FLOAT64, "DOUBLE");
        assertPrimitiveMapping(Type.BOOLEAN, "TINYINT");
        assertPrimitiveMapping(Type.BYTES, "VARBINARY(1024)");
        assertPrimitiveMapping(Type.STRING, "VARCHAR(256)");
    }

    @Test
    public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
        assertDecimalMapping(0, "DECIMAL(65,0)");
        assertDecimalMapping(3, "DECIMAL(65,3)");
        assertDecimalMapping(4, "DECIMAL(65,4)");
        assertDecimalMapping(5, "DECIMAL(65,5)");
    }

    @Test
    public void shouldMapDataTypes() {
        verifyDataTypeMapping("TINYINT", Schema.INT8_SCHEMA);
        verifyDataTypeMapping("SMALLINT", Schema.INT16_SCHEMA);
        verifyDataTypeMapping("INT", Schema.INT32_SCHEMA);
        verifyDataTypeMapping("BIGINT", Schema.INT64_SCHEMA);
        verifyDataTypeMapping("FLOAT", Schema.FLOAT32_SCHEMA);
        verifyDataTypeMapping("DOUBLE", Schema.FLOAT64_SCHEMA);
        verifyDataTypeMapping("TINYINT", Schema.BOOLEAN_SCHEMA);
        verifyDataTypeMapping("VARCHAR(256)", Schema.STRING_SCHEMA);
        verifyDataTypeMapping("VARBINARY(1024)", Schema.BYTES_SCHEMA);
        verifyDataTypeMapping("DECIMAL(65,0)", Decimal.schema(0));
        verifyDataTypeMapping("DECIMAL(65,2)", Decimal.schema(2));
        verifyDataTypeMapping("DATE", Date.SCHEMA);
        verifyDataTypeMapping("TIME(3)", Time.SCHEMA);
        verifyDataTypeMapping("DATETIME(3)", Timestamp.SCHEMA);
    }

    @Test
    public void shouldMapDateSchemaTypeToDateSqlType() {
        assertDateMapping("DATE");
    }

    @Test
    public void shouldMapTimeSchemaTypeToTimeSqlType() {
        assertTimeMapping("TIME(3)");
    }

    @Test
    public void shouldMapTimestampSchemaTypeToTimestampSqlType() {
        assertTimestampMapping("DATETIME(3)");
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
        final TableId actor = tableId("actor");
        final String actual = dialect.buildUpsertQueryStatement(
            actor,
            columns(actor, "actor_id"),
            columns(actor, "first_name", "last_name", "score")
        );
        assertQueryEquals(expected, actual);
    }

    @Test
    public void upsertOnlyKeyCols() {
        final String expected = readQueryResourceForThisTest("upsert_only_key_cols");
        final TableId actor = tableId("actor");
        final String actual = dialect.buildUpsertQueryStatement(
            actor,
            columns(actor, "actor_id"),
            columns(actor)
        );
        assertQueryEquals(expected, actual);
    }

    @Test
    public void insert() {
        final String expected = readQueryResourceForThisTest("insert");
        final TableId customers = tableId("customers");
        final String actual = dialect.buildInsertStatement(
            customers,
            columns(customers),
            columns(customers, "age", "firstName", "lastName")
        );
        assertQueryEquals(expected, actual);
    }

    @Test
    public void update() {
        final String expected = readQueryResourceForThisTest("update");
        final TableId customers = tableId("customers");
        final String actual = dialect.buildUpdateStatement(
            customers,
            columns(customers, "id"),
            columns(customers, "age", "firstName", "lastName")
        );
        assertQueryEquals(expected, actual);
    }

    @Test
    public void shouldSanitizeUrlWithCredentialsInHosts() {
        assertSanitizedUrl(
            "mysqlx://sandy:secret@(host=myhost1,port=1111)/db?key1=value1",
            "mysqlx://sandy:****@(host=myhost1,port=1111)/db?key1=value1"
        );
    }

    @Test
    public void shouldSanitizeUrlWithCredentialsInProperties() {
        assertSanitizedUrl(
            "jdbc:mysql://[(host=myhost1,port=1111,user=sandy,password=secret),"
                + "(password=secret,host=myhost2,port=2222,user=finn,password=secret)]/db",
            "jdbc:mysql://[(host=myhost1,port=1111,user=sandy,password=****),"
                + "(password=****,host=myhost2,port=2222,user=finn,password=****)]/db"
        );
    }

    @Test
    public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
        assertSanitizedUrl(
            "jdbc:mysql://(host=myhost1,port=1111),(host=myhost2,port=2222)/"
                + "db?password=secret&key1=value1&key2=value2&key3=value3&"
                + "user=smith&password=secret&other=value",
            "jdbc:mysql://(host=myhost1,port=1111),(host=myhost2,port=2222)/"
                + "db?password=****&key1=value1&key2=value2&key3=value3&"
                + "user=smith&password=****&other=value"
        );
    }
}
