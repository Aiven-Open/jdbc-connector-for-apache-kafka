/**
 * Copyright 2015 Datamountaineer.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.datamountaineer.streamreactor.connect.jdbc.common;

import com.datamountaineer.streamreactor.connect.jdbc.AutoCloseableHelper;
import com.datamountaineer.streamreactor.connect.jdbc.ConnectionProvider;
import com.datamountaineer.streamreactor.connect.jdbc.sink.SinkRecordField;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
    logger.info("Updating local database metadata for " + table);
    tables.put(table.getName(), table);
  }

  public Changes getChanges(final Map<String, Collection<SinkRecordField>> tableColumnsMap) {
    ParameterValidator.notNull(tableColumnsMap, "tableColumnsMap");
    Map<String, Collection<SinkRecordField>> created = null;
    Map<String, Collection<SinkRecordField>> amended = null;
    for (final Map.Entry<String, Collection<SinkRecordField>> entry : tableColumnsMap.entrySet()) {
      final DbTable table = tables.get(entry.getKey());
      if (table == null) {
        //we don't have this table
        if (created == null) created = new HashMap<>();
        created.put(entry.getKey(), tableColumnsMap.get(entry.getKey()));
      } else {
        final Map<String, DbTableColumn> existingColumnsMap = table.getColumns();
        for (final SinkRecordField field : entry.getValue()) {
          if (!existingColumnsMap.containsKey(field.getName())) {
            if (amended == null) {
              amended = new HashMap<>();
            }
            Collection<SinkRecordField> newFileds = amended.get(table.getName());
            //new field which hasn't been seen before
            if (newFileds == null) {
              logger.info(String.format("Detected new field %s for table %s in SinkRecord.", field.getName(), table.getName()));
              newFileds = new ArrayList<>();
              amended.put(table.getName(), newFileds);
            }
            newFileds.add(field);
          }
        }
      }
    }
    return new Changes(amended, created);
  }

  /**
   * Returns true if the given table name is present in the local cache for the database available tables.
   *
   * @param tableName - The table name to check is present
   * @return true if table is present; false otherwise
   */
  public boolean containsTable(final String tableName) {
    return tables.containsKey(tableName);
  }

  /**
   * Returns a sequence of database available table names
   *
   * @return All the table names
   */
  public Collection<String> getTableNames() {
    return tables.keySet();
  }

  /**
   * Contains the changes related to a new set of SinkRecord sent to the sink.
   */
  public final class Changes {
    private final Map<String, Collection<SinkRecordField>> amendmentMap;
    private final Map<String, Collection<SinkRecordField>> createdMap;

    public Changes(Map<String, Collection<SinkRecordField>> amendmentMap, Map<String, Collection<SinkRecordField>> createdMap) {
      this.amendmentMap = amendmentMap;
      this.createdMap = createdMap;
    }

    /**
     * Returns a sequence of tables whose schema has to be changed by adding new columns.
     *
     * @return The tables and their columns
     */
    public Map<String, Collection<SinkRecordField>> getAmendmentMap() {
      return amendmentMap;
    }

    /**
     * Returns a sequence of tables to be created.
     *
     * @return The tables and their columns
     */
    public Map<String, Collection<SinkRecordField>> getCreatedMap() {
      return createdMap;
    }
  }

  /**
   * Creates a DatabaseMetadata instance for the tables given
   *
   * @param connectionProvider - Database connection pooling
   * @param tables             - The tables to consider
   * @return The database metadata
   */
  public static DatabaseMetadata getDatabaseMetadata(final ConnectionProvider connectionProvider,
                                                     final Set<String> tables) {
    ParameterValidator.notNull(connectionProvider, "connectionProvider");
    ParameterValidator.notNull(tables, "tables");
    if (tables.isEmpty()) {
      throw new IllegalArgumentException("<tables> parameter is empty");
    }
    Connection connection = null;
    try {
      connection = connectionProvider.getConnection();

      final String catalog = connection.getCatalog();

      final DatabaseMetaData dbMetadata = connection.getMetaData();

      final List<DbTable> dbTables = Lists.newArrayList();
      for (final String table : tables) {
        if (tableExists(connection, catalog, table)) {
          final List<DbTableColumn> columns = getTableColumns(catalog, table, dbMetadata);
          dbTables.add(new DbTable(table, columns));
        }
      }

      return new DatabaseMetadata(catalog, dbTables);
    } catch (SQLException ex) {
      logger.error(
              String.format("An error occurred trying to retrieve the database metadata for the given tables.%s", ex.getMessage()),
              ex);
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

  /**
   * Checks the given table is present in the database.
   *
   * @param connection - Database connection instance
   * @param tableName  - The table to check if it is present or not
   * @return true if the table is present; false otherwise
   * @throws SQLException
   */
  public static boolean tableExists(final Connection connection,
                                    final String tableName) throws SQLException {
    final String catalog = connection.getCatalog();
    return tableExists(connection, catalog, tableName);
  }

  /**
   * Checks the given table is present in the database
   *
   * @param connection _ The database connection instance
   * @param catalog    - The database name
   * @param tableName  - The table to check if it is present or not
   * @return true the table is present; false otherwise
   * @throws SQLException
   */
  private static boolean tableExists(final Connection connection,
                                     final String catalog,
                                     final String tableName) throws SQLException {
    ParameterValidator.notNull(connection, "connection");
    ParameterValidator.notNull(tableName, "tableName");


    DatabaseMetaData meta = connection.getMetaData();

    ResultSet rs = null;
    try {

      final String product = meta.getDatabaseProductName();

      if (product.toLowerCase().equals("oracle")) {
        String schema = getOracleSchema(connection);
        logger.info(String.format("[%s] Checking %s exists for catalog=%s and schema %s", product, tableName, catalog, schema));
        rs = meta.getTables(catalog, schema.toUpperCase(), tableName, new String[]{"TABLE"});
      } else if (product.toLowerCase().contains("postgre")) {
        String schema = connection.getSchema();
        logger.info(String.format("[%s] Checking %s exists for catalog=%s and schema %s", product, tableName, catalog, schema));
        rs = meta.getTables(catalog, schema, tableName, new String[]{"TABLE"});
      } else {
        logger.info(String.format("[%s] Checking %s exists for catalog=%s and schema %s", product, tableName, catalog, ""));
        rs = meta.getTables(catalog, null, tableName, new String[]{"TABLE"});
      }

      boolean exists = rs.next();

      logger.info(String.format("Table '%s' is%s present in catalog=%s - [%s]", tableName, exists ? "" : " not", catalog, product));

      return exists;
    } finally {
      AutoCloseableHelper.close(rs);
    }
  }


  /**
   * Uses sql to retrieve the database name for Oracle. The jdbc API is not working properly.
   *
   * @param connection - The instance for the jdbc Connection
   * @return -The database name
   * @throws SQLException
   */
  private static String getOracleSchema(final Connection connection) throws SQLException {
    Statement statement = null;
    ResultSet rs = null;
    try {
      statement = connection.createStatement();
      rs = statement.executeQuery("select sys_context('userenv','current_schema') x from dual");
      rs.next();
      return rs.getString("x");
    } finally {
      AutoCloseableHelper.close(rs);

      AutoCloseableHelper.close(statement);
    }
  }

  /***
   * Returns the tables information
   *
   * @param connectionProvider - An instance of the ConnectionProvider
   * @return The information related to all the table present in the database
   */
  public static List<DbTable> getTableMetadata(final ConnectionProvider connectionProvider) {
    Connection connection = null;
    try {
      connection = connectionProvider.getConnection();

      final String catalog = connection.getCatalog();
      final DatabaseMetaData dbMetadata = connection.getMetaData();
      final String schema = dbMetadata.getUserName();
      ResultSet tablesRs = dbMetadata.getTables(catalog, dbMetadata.getUserName(), schema, new String[]{"TABLE"});

      final List<DbTable> tables = new ArrayList<>();
      while (tablesRs.next()) {
        final String tableName = tablesRs.getString("TABLE_NAME");
        final List<DbTableColumn> columns = getTableColumns(catalog, tableName, dbMetadata);

        tables.add(new DbTable(tableName, columns));
      }
      return tables;
    } catch (SQLException ex) {
      logger.error(String.format("An error has occurred trying to retrieve the tables metadata.%s", ex.getMessage()), ex);
      throw new RuntimeException("Sql exception occurred.", ex);
    } finally {
      AutoCloseableHelper.close(connection);
    }
  }

  /***
   * Returns the tables information
   *
   * @param connection - The instance of the jdbc Connection
   * @param tableName  -The table for which to get the column information
   * @return The information related to the table columns
   */
  public static DbTable getTableMetadata(final Connection connection, final String tableName) throws SQLException {

    final String catalog = connection.getCatalog();
    final DatabaseMetaData dbMetadata = connection.getMetaData();
    final List<DbTableColumn> columns = getTableColumns(catalog, tableName, dbMetadata);
    return new DbTable(tableName, columns);
  }

  private static List<DbTableColumn> getTableColumns(final String catalog, final String tableName, final DatabaseMetaData dbMetaData) throws SQLException {

    final String product = dbMetaData.getDatabaseProductName();
    Connection connection = dbMetaData.getConnection();

    ResultSet nonPKcolumnsRS = null;
    ResultSet pkColumnsRS = null;

    if (product.toLowerCase().equals("oracle")) {
      String schema = getOracleSchema(connection);
      logger.info(String.format("[%s] Checking columns exists for table='%s', schema '%s' and catalog '%s'",
              product, tableName, schema, catalog));

      nonPKcolumnsRS = dbMetaData.getColumns(catalog, schema.toUpperCase(), tableName.toUpperCase(), null);
      pkColumnsRS = dbMetaData.getPrimaryKeys(catalog, schema.toUpperCase(), tableName.toUpperCase());

    } else if (product.toLowerCase().contains("postgre")) {
      String schema = connection.getSchema();
      nonPKcolumnsRS = dbMetaData.getColumns(catalog, schema, tableName, null);

      pkColumnsRS = dbMetaData.getPrimaryKeys(catalog, schema, tableName);
    } else {
      nonPKcolumnsRS = dbMetaData.getColumns(catalog, null, tableName, null);

      pkColumnsRS = dbMetaData.getPrimaryKeys(catalog, null, tableName);
    }

    //final ResultSet nonPKcolumnsRS = dbMetaData.getColumns(catalog, schema, tableName, null);
    final List<DbTableColumn> columns = new ArrayList<>();

    //final ResultSet pkColumnsRS = dbMetaData.getPrimaryKeys(catalog, schema, tableName);
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