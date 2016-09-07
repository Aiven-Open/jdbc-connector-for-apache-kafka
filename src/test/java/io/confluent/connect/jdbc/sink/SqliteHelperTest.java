/*
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

package io.confluent.connect.jdbc.sink;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.jdbc.sink.metadata.DbTable;
import io.confluent.connect.jdbc.sink.metadata.DbTableColumn;
import io.confluent.connect.jdbc.util.JdbcUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
    String createEmployees = "CREATE TABLE employees\n" +
                             "( employee_id INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
                             "  last_name VARCHAR NOT NULL,\n" +
                             "  first_name VARCHAR,\n" +
                             "  hire_date DATE\n" +
                             ");";

    String createProducts = "CREATE TABLE products\n" +
                            "( product_id INTEGER PRIMARY KEY AUTOINCREMENT,\n" +
                            "  product_name VARCHAR NOT NULL,\n" +
                            "  quantity INTEGER NOT NULL DEFAULT 0\n" +
                            ");";

    String createNonPkTable = "CREATE TABLE nonPk (id numeric, response text)";

    sqliteHelper.createTable(createEmployees);
    sqliteHelper.createTable(createProducts);
    sqliteHelper.createTable(createNonPkTable);

    final Map<String, DbTable> tables = new HashMap<>();
    for (String tableName : JdbcUtils.getTables(sqliteHelper.connection, null)) {
      tables.put(tableName, DbMetadataQueries.getTableMetadata(sqliteHelper.connection, tableName));
    }

    assertEquals(tables.size(), 3);
    assertTrue(tables.containsKey("employees"));
    assertTrue(tables.containsKey("products"));
    assertTrue(tables.containsKey("nonPk"));

    DbTable nonPk = tables.get("nonPk");

    Map<String, DbTableColumn> columns = nonPk.columns;
    assertEquals(columns.size(), 2);
    assertTrue(columns.containsKey("id"));
    assertTrue(columns.get("id").allowsNull);
    assertFalse(columns.get("id").isPrimaryKey);
    assertEquals(columns.get("id").sqlType, Types.FLOAT);
    assertTrue(columns.containsKey("response"));
    assertTrue(columns.get("response").allowsNull);
    assertFalse(columns.get("response").isPrimaryKey);
    assertEquals(columns.get("response").sqlType, Types.VARCHAR);

    DbTable employees = tables.get("employees");
    columns = employees.columns;
    assertEquals(columns.size(), 4);
    assertTrue(columns.containsKey("employee_id"));
    assertFalse(columns.get("employee_id").allowsNull);
    assertTrue(columns.get("employee_id").isPrimaryKey);
    assertEquals(columns.get("employee_id").sqlType, Types.INTEGER);
    assertTrue(columns.containsKey("last_name"));
    assertFalse(columns.get("last_name").allowsNull);
    assertFalse(columns.get("last_name").isPrimaryKey);
    assertEquals(columns.get("last_name").sqlType, Types.VARCHAR);
    assertTrue(columns.containsKey("first_name"));
    assertTrue(columns.get("first_name").allowsNull);
    assertFalse(columns.get("first_name").isPrimaryKey);
    assertEquals(columns.get("first_name").sqlType, Types.VARCHAR);
    assertTrue(columns.containsKey("hire_date"));
    assertTrue(columns.get("hire_date").allowsNull);
    assertFalse(columns.get("hire_date").isPrimaryKey);
    // sqlite returns VARCHAR for DATE. why?!
    // assertEquals(columns.get("hire_date").getSqlType(), Types.DATE);

    DbTable products = tables.get("products");
    columns = products.columns;
    assertEquals(columns.size(), 3);
    assertTrue(columns.containsKey("product_id"));
    assertFalse(columns.get("product_id").allowsNull);
    assertTrue(columns.get("product_id").isPrimaryKey);
    assertEquals(columns.get("product_id").sqlType, Types.INTEGER);

    assertTrue(columns.containsKey("product_name"));
    assertFalse(columns.get("product_name").allowsNull);
    assertFalse(columns.get("product_name").isPrimaryKey);
    assertEquals(columns.get("product_name").sqlType, Types.VARCHAR);

    assertTrue(columns.containsKey("quantity"));
    assertFalse(columns.get("quantity").allowsNull);
    assertFalse(columns.get("quantity").isPrimaryKey);
    assertEquals(columns.get("quantity").sqlType, Types.INTEGER);
  }
}
