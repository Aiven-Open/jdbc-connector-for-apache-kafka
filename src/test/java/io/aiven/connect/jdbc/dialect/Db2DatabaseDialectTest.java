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

public class Db2DatabaseDialectTest extends BaseDialectTest<Db2DatabaseDialect> {

    @Override
    protected Db2DatabaseDialect createDialect() {
        return new Db2DatabaseDialect(sourceConfigWithUrl("jdbc:db2://something"));
    }

    @Test
    public void shouldMapPrimitiveSchemaTypeToSqlTypes() {
        assertPrimitiveMapping(Type.INT8, "SMALLINT");
        assertPrimitiveMapping(Type.INT16, "SMALLINT");
        assertPrimitiveMapping(Type.INT32, "INTEGER");
        assertPrimitiveMapping(Type.INT64, "BIGINT");
        assertPrimitiveMapping(Type.FLOAT32, "FLOAT");
        assertPrimitiveMapping(Type.FLOAT64, "DOUBLE");
        assertPrimitiveMapping(Type.BOOLEAN, "SMALLINT");
        assertPrimitiveMapping(Type.BYTES, "BLOB(64000)");
        assertPrimitiveMapping(Type.STRING, "VARCHAR(32672)");
    }

    @Test
    public void shouldMapDecimalSchemaTypeToDecimalSqlType() {
        assertDecimalMapping(0, "DECIMAL(31,0)");
        assertDecimalMapping(5, "DECIMAL(31,5)");
    }

    @Test
    public void shouldMapDataTypes() {
        verifyDataTypeMapping("SMALLINT", Schema.INT8_SCHEMA);
        verifyDataTypeMapping("SMALLINT", Schema.INT16_SCHEMA);
        verifyDataTypeMapping("INTEGER", Schema.INT32_SCHEMA);
        verifyDataTypeMapping("BIGINT", Schema.INT64_SCHEMA);
        verifyDataTypeMapping("FLOAT", Schema.FLOAT32_SCHEMA);
        verifyDataTypeMapping("DOUBLE", Schema.FLOAT64_SCHEMA);
        verifyDataTypeMapping("SMALLINT", Schema.BOOLEAN_SCHEMA);
        verifyDataTypeMapping("VARCHAR(32672)", Schema.STRING_SCHEMA);
        verifyDataTypeMapping("BLOB(64000)", Schema.BYTES_SCHEMA);
        verifyDataTypeMapping("DECIMAL(31,0)", Decimal.schema(0));
        verifyDataTypeMapping("DECIMAL(31,2)", Decimal.schema(2));
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
        final String[] expected = {
            readQueryResourceForThisTest("alter_table")
        };
        final List<String> actual = dialect.buildAlterTable(tableId, sinkRecordFields);
        assertStatements(expected, actual);
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
    public void shouldBuildUpsertStatement() {
        final String expected = readQueryResourceForThisTest("upsert0");
        final String actual = dialect.buildUpsertQueryStatement(tableId, pkColumns, columnsAtoD);
        assertQueryEquals(expected, actual);
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
    public void shouldSanitizeUrlWithoutCredentialsInProperties() {
        assertSanitizedUrl(
            "jdbc:db2://sysmvs1.stl.ibm.com:5021/STLEC1:user=dbadm;other=dbadm;"
                + "traceLevel=all",
            "jdbc:db2://sysmvs1.stl.ibm.com:5021/STLEC1:user=dbadm;other=dbadm;"
                + "traceLevel=all"
        );
    }

    @Test
    public void shouldSanitizeUrlWithCredentialsInUrlProperties() {
        assertSanitizedUrl(
            "jdbc:db2://sysmvs1.stl.ibm.com:5021/STLEC1:user=dbadm;password=dbadm;"
                + "traceLevel=all",
            "jdbc:db2://sysmvs1.stl.ibm.com:5021/STLEC1:user=dbadm;password=****;"
                + "traceLevel=all"
        );
        assertSanitizedUrl(
            "jdbc:db2://sysmvs1.stl.ibm.com:5021/STLEC1:password=dbadm;user=dbadm;"
                + "traceLevel=all",
            "jdbc:db2://sysmvs1.stl.ibm.com:5021/STLEC1:password=****;user=dbadm;"
                + "traceLevel=all"
        );
    }
}
