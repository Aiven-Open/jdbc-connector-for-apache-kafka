/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2016 Confluent Inc.
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

package io.aiven.connect.jdbc.sink;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import io.aiven.connect.jdbc.config.JdbcConfig;
import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.dialect.SqliteDatabaseDialect;
import io.aiven.connect.jdbc.util.ColumnDefinition;
import io.aiven.connect.jdbc.util.TableDefinition;
import io.aiven.connect.jdbc.util.TableId;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SqliteHelperTest {

    private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());

    @Before
    public void setUp() throws IOException, SQLException {
        sqliteHelper.setUp();
    }

    @After
    public void tearDown() throws IOException, SQLException {
        sqliteHelper.tearDown();
    }

    @Test
    public void returnTheDatabaseTableInformation() throws SQLException {
        final String createEmployees = "CREATE TABLE employees\n"
            + "( employee_id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
            + "  last_name VARCHAR NOT NULL,\n"
            + "  first_name VARCHAR,\n"
            + "  hire_date DATE\n"
            + ");";

        final String createProducts = "CREATE TABLE products\n"
            + "( product_id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
            + "  product_name VARCHAR NOT NULL,\n"
            + "  quantity INTEGER NOT NULL DEFAULT 0\n"
            + ");";

        final String createNonPkTable = "CREATE TABLE nonPk (id numeric, response text)";

        sqliteHelper.createTable(createEmployees);
        sqliteHelper.createTable(createProducts);
        sqliteHelper.createTable(createNonPkTable);

        final Map<String, String> connProps = new HashMap<>();
        connProps.put(JdbcConfig.CONNECTION_URL_CONFIG, sqliteHelper.sqliteUri());
        final JdbcSinkConfig config = new JdbcSinkConfig(connProps);
        final DatabaseDialect dialect = new SqliteDatabaseDialect(config);

        final Map<String, TableDefinition> tables = new HashMap<>();
        for (final TableId tableId : dialect.tableIds(sqliteHelper.connection)) {
            tables.put(tableId.tableName(), dialect.describeTable(sqliteHelper.connection, tableId));
        }

        assertThat(tables).containsOnlyKeys("employees", "products", "nonPk");

        final TableDefinition nonPk = tables.get("nonPk");
        assertThat(nonPk.columnCount()).isEqualTo(2);

        ColumnDefinition colDefn = nonPk.definitionForColumn("id");
        assertThat(colDefn.isOptional()).isTrue();
        assertThat(colDefn.isPrimaryKey()).isFalse();
        assertThat(colDefn.type()).isEqualTo(Types.FLOAT);

        colDefn = nonPk.definitionForColumn("response");
        assertThat(colDefn.isOptional()).isTrue();
        assertThat(colDefn.isPrimaryKey()).isFalse();
        assertThat(colDefn.type()).isEqualTo(Types.VARCHAR);

        final TableDefinition employees = tables.get("employees");
        assertThat(employees.columnCount()).isEqualTo(4);

        assertThat(employees.definitionForColumn("employee_id")).isNotNull();
        assertThat(employees.definitionForColumn("employee_id").isOptional()).isFalse();
        assertThat(employees.definitionForColumn("employee_id").isPrimaryKey()).isTrue();
        assertThat(employees.definitionForColumn("employee_id").type()).isEqualTo(Types.INTEGER);
        assertThat(employees.definitionForColumn("last_name")).isNotNull();
        assertThat(employees.definitionForColumn("last_name").isOptional()).isFalse();
        assertThat(employees.definitionForColumn("last_name").isPrimaryKey()).isFalse();
        assertThat(employees.definitionForColumn("last_name").type()).isEqualTo(Types.VARCHAR);
        assertThat(employees.definitionForColumn("first_name")).isNotNull();
        assertThat(employees.definitionForColumn("first_name").isOptional()).isTrue();
        assertThat(employees.definitionForColumn("first_name").isPrimaryKey()).isFalse();
        assertThat(employees.definitionForColumn("first_name").type()).isEqualTo(Types.VARCHAR);
        assertThat(employees.definitionForColumn("hire_date")).isNotNull();
        assertThat(employees.definitionForColumn("hire_date").isOptional()).isTrue();
        assertThat(employees.definitionForColumn("hire_date").isPrimaryKey()).isFalse();
        // sqlite returns VARCHAR for DATE. why?!
        assertThat(employees.definitionForColumn("hire_date").type()).isEqualTo(Types.VARCHAR);
        // assertEquals(columns.get("hire_date").getSqlType(), Types.DATE);

        final TableDefinition products = tables.get("products");
        assertThat(employees.columnCount()).isEqualTo(4);

        assertThat(products.definitionForColumn("product_id")).isNotNull();
        assertThat(products.definitionForColumn("product_id").isOptional()).isFalse();
        assertThat(products.definitionForColumn("product_id").isPrimaryKey()).isTrue();
        assertThat(products.definitionForColumn("product_id").type()).isEqualTo(Types.INTEGER);
        assertThat(products.definitionForColumn("product_name")).isNotNull();
        assertThat(products.definitionForColumn("product_name").isOptional()).isFalse();
        assertThat(products.definitionForColumn("product_name").isPrimaryKey()).isFalse();
        assertThat(products.definitionForColumn("product_name").type()).isEqualTo(Types.VARCHAR);
        assertThat(products.definitionForColumn("quantity")).isNotNull();
        assertThat(products.definitionForColumn("quantity").isOptional()).isFalse();
        assertThat(products.definitionForColumn("quantity").isPrimaryKey()).isFalse();
        assertThat(products.definitionForColumn("quantity").type()).isEqualTo(Types.INTEGER);
    }
}
