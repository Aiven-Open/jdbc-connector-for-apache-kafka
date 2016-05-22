/**
 * Copyright 2015 Datamountaineer.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.jdbc.sink.common.ParameterValidator;
import com.google.common.collect.Lists;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Contains all the database tables metadata.
 */
public class DatabaseMetadata {
  private static final Logger logger = LoggerFactory.getLogger(DatabaseMetadata.class);
  private final String databaseName;
  private final Map<String, DbTable> tables;

  public DatabaseMetadata(final String databaseName, final List<DbTable> tables) {
    //we support null because SqLite does return as database
    if (databaseName != null && databaseName.trim().length() == 0) {
      throw new IllegalArgumentException("<databasename> is not valid.");
    }
    ParameterValidator.notNull(tables, "tables");
    this.databaseName = databaseName;
    this.tables = new HashMap<>();
    for (final DbTable t : tables) {
      this.tables.put(t.getName(), t);
    }
  }

  public DbTable getTable(final String tableName) {
    return tables.get(tableName);
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void update(final DbTable table) {
    tables.put(table.getName(), table);
  }

  public Changes getChanges(final Map<String, Collection<Field>> tableColumnsMap) {
    ParameterValidator.notNull(tableColumnsMap, "tableColumnsMap");
    Map<String, Collection<Field>> created = null;
    Map<String, Collection<Field>> amended = null;
    for (final Map.Entry<String, Collection<Field>> entry : tableColumnsMap.entrySet()) {
      if (!tables.containsKey(entry.getKey())) {
        //we don't have this table
        if (created == null) created = new HashMap<>();
        created.put(entry.getKey(), tableColumnsMap.get(entry.getKey()));
      } else {
        final DbTable table = tables.get(entry.getKey());
        final Map<String, DbTableColumn> existingColumnsMap = table.getColumns();
        for (final Field field : entry.getValue()) {
          if (!existingColumnsMap.containsKey(field.getName())) {
            if (amended == null) {
              amended = new HashMap<>();
            }
            //new field which hasn't been seen before
            if (!amended.containsKey(table.getName())) {
              amended.put(table.getName(), new ArrayList<Field>());
            }

            final Collection<Field> newFileds = amended.get(table.getName());
            newFileds.add(field);
          }
        }
      }
    }
    return new Changes(amended, created);
  }

  public boolean containsTable(final String tableName) {
    return tables.containsKey(tableName);
  }

  public Collection<String> getTableNames() {
    return tables.keySet();
  }

  public final class Changes {
    private final Map<String, Collection<Field>> amendmentMap;
    private final Map<String, Collection<Field>> createdMap;

    public Changes(Map<String, Collection<Field>> amendmentMap, Map<String, Collection<Field>> createdMap) {
      this.amendmentMap = amendmentMap;
      this.createdMap = createdMap;
    }

    public Map<String, Collection<Field>> getAmendmentMap() {
      return amendmentMap;
    }

    public Map<String, Collection<Field>> getCreatedMap() {
      return createdMap;
    }
  }

  /**
   * Creates a DatabaseMetadata instance for the tables given
   *
   * @param connectionPooling - Database connection pooling
   * @param tables            - The tables to consider
   * @return The database metadata
   */
  public static DatabaseMetadata getDatabaseMetadata(final HikariDataSource connectionPooling,
                                                     final Set<String> tables) {
    ParameterValidator.notNull(connectionPooling, "hikariDataSource");
    ParameterValidator.notNull(tables, "tables");
    if (tables.isEmpty()) {
      throw new IllegalArgumentException("<tables> parameter is empty");
    }
    Connection connection = null;
    try {
      connection = connectionPooling.getConnection();

      final String catalog = connection.getCatalog();
      final DatabaseMetaData dbMetadata = connection.getMetaData();

      final List<DbTable> dbTables = Lists.newArrayList();
      for (final String table : tables) {
        final List<DbTableColumn> columns = getTableColumns(catalog, table, dbMetadata);
        dbTables.add(new DbTable(table, columns));
      }

      return new DatabaseMetadata(catalog, dbTables);
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (Throwable t) {
          logger.error(t.getMessage(), t);
        }
      }
    }
  }

  public static boolean tableExists(final Connection connection,
                                    final String tableName) throws SQLException {
    ParameterValidator.notNull(connection, "connection");
    ParameterValidator.notNull(tableName, "tableName");
    DatabaseMetaData meta = connection.getMetaData();

    ResultSet rs = null;
    try {
      rs = meta.getTables(null, null, tableName, new String[]{"TABLE"});
      return rs.next();
    } finally {
      if (rs != null) {
        rs.close();
      }
    }
  }

  /***
   * Returns the tables information
   *
   * @param connectionPool
   * @return
   */
  public static List<DbTable> getTableMetadata(final HikariDataSource connectionPool) {
    Connection connection = null;
    try {
      connection = connectionPool.getConnection();

      final String catalog = connection.getCatalog();
      final DatabaseMetaData dbMetadata = connection.getMetaData();
      ResultSet tablesRs = dbMetadata.getTables(catalog, null, null, new String[]{"TABLE"});

      final List<DbTable> tables = new ArrayList<>();
      while (tablesRs.next()) {
        final String tableName = tablesRs.getString("TABLE_NAME");
        final List<DbTableColumn> columns = getTableColumns(catalog, tableName, dbMetadata);

        tables.add(new DbTable(tableName, columns));
      }
      return tables;
    } catch (SQLException ex) {
      throw new RuntimeException("Sql exception occured.", ex);
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (Throwable t) {
          logger.error(t.getMessage(), t);
        }
      }
    }
  }

  /***
   * Returns the tables information
   *
   * @param connection
   * @return
   */
  public static DbTable getTableMetadata(final Connection connection, final String tableName) throws SQLException {

    final String catalog = connection.getCatalog();
    final DatabaseMetaData dbMetadata = connection.getMetaData();
    final List<DbTableColumn> columns = getTableColumns(catalog, tableName, dbMetadata);
    return new DbTable(tableName, columns);
  }

  private static List<DbTableColumn> getTableColumns(final String catalog, final String tableName, final DatabaseMetaData dbMetaData) throws SQLException {
    final ResultSet nonPKcolumnsRS = dbMetaData.getColumns(catalog, null, tableName, null);
    final List<DbTableColumn> columns = new ArrayList<>();

    final ResultSet pkColumnsRS = dbMetaData.getPrimaryKeys(catalog, null, tableName);
    final Set<String> pkColumns = new HashSet<>();

    while (pkColumnsRS.next()) {
      final String colName = pkColumnsRS.getString("COLUMN_NAME");
      pkColumns.add(colName);
    }

    while (nonPKcolumnsRS.next()) {
      final String colName = nonPKcolumnsRS.getString("COLUMN_NAME");
      final int sqlType = nonPKcolumnsRS.getInt("DATA_TYPE");
      boolean isNullable = !pkColumns.contains(colName);
      //nonPKcolumnsRS.getInt("NULLABLE") == DatabaseMetaData.attributeNullable;
      //sqlite reports in this case for PK as true allows nullable
      if (isNullable)
        isNullable = Objects.equals("YES", nonPKcolumnsRS.getString("IS_NULLABLE"));

      columns.add(new DbTableColumn(colName, pkColumns.contains(colName), isNullable, sqlType));
    }
    return columns;
  }
}