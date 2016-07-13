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
    assertFalse(DbMetadataQueries.tableExists(sqliteHelper.connection, "somerandomtable"));
  }

  @Test
  public void tableExists() throws SQLException {
    sqliteHelper.createTable("create table somerandomtable (id int)");
    assertTrue(DbMetadataQueries.tableExists(sqliteHelper.connection, "somerandomtable"));
    assertFalse(DbMetadataQueries.tableExists(sqliteHelper.connection, "someotherrandomtable"));
  }

  @Test(expected =  SQLException.class)
  public void tableOnEmptyDb() throws SQLException {
    DbMetadataQueries.table(sqliteHelper.connection, "somerand");
  }

  @Test
  public void basicTable() throws SQLException {
    sqliteHelper.createTable("create table x (id int primary key, name text not null, optional_age int null)");
    DbTable metadata = DbMetadataQueries.table(sqliteHelper.connection, "x");
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
