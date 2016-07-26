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
import java.util.Collections;
import java.util.Map;

import io.confluent.connect.jdbc.sink.metadata.DbTable;
import io.confluent.connect.jdbc.sink.metadata.DbTableColumn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DbMetadataQueriesTest {

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
  public void tableExistsOnEmptyDb() throws SQLException {
    assertFalse(DbMetadataQueries.doesTableExist(sqliteHelper.connection, "somerandomtable"));
  }

  @Test
  public void tableExists() throws SQLException {
    sqliteHelper.createTable("create table somerandomtable (id int)");
    assertTrue(DbMetadataQueries.doesTableExist(sqliteHelper.connection, "somerandomtable"));
    assertFalse(DbMetadataQueries.doesTableExist(sqliteHelper.connection, "someotherrandomtable"));
  }

  @Test(expected =  SQLException.class)
  public void tableOnEmptyDb() throws SQLException {
    DbMetadataQueries.getTableMetadata(sqliteHelper.connection, "somerand");
  }

  @Test
  public void basicTable() throws SQLException {
    sqliteHelper.createTable("create table x (id int primary key, name text not null, optional_age int null)");
    DbTable metadata = DbMetadataQueries.getTableMetadata(sqliteHelper.connection, "x");
    assertEquals(metadata.name, "x");

    assertEquals(Collections.singleton("id"), metadata.primaryKeyColumnNames);

    Map<String, DbTableColumn> columns = metadata.columns;
    assertEquals(3, columns.size());

    DbTableColumn idCol = columns.get("id");
    assertEquals("id", idCol.name);
    assertTrue(idCol.isPrimaryKey);
    assertFalse(idCol.allowsNull);
    assertEquals(Types.INTEGER, idCol.sqlType);

    DbTableColumn textCol = columns.get("name");
    assertFalse(textCol.isPrimaryKey);
    assertFalse(textCol.allowsNull);
    assertEquals(Types.VARCHAR, textCol.sqlType);

    DbTableColumn ageCol = columns.get("optional_age");
    assertFalse(ageCol.isPrimaryKey);
    assertTrue(ageCol.allowsNull);
    assertEquals(Types.INTEGER, ageCol.sqlType);
  }

}
