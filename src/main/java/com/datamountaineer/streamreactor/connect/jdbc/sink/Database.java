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

import com.datamountaineer.streamreactor.connect.jdbc.common.DatabaseMetadata;
import com.datamountaineer.streamreactor.connect.jdbc.common.DbTable;
import com.datamountaineer.streamreactor.connect.jdbc.common.ParameterValidator;
import com.datamountaineer.streamreactor.connect.jdbc.dialect.DbDialect;
import com.zaxxer.hikari.HikariDataSource;
import io.confluent.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Controls the database changes - creating/amending tables.
 */
public class Database {
  private final static Logger logger = LoggerFactory.getLogger(Database.class);

  private final int executionRetries;
  private final Set<String> tablesAllowingAutoCreate;
  private final Set<String> tablesAllowingSchemaEvolution;
  private final DatabaseMetadata databaseMetadata;
  private final DbDialect dbDialect;
  private final HikariDataSource connectionPool;

  public Database(final HikariDataSource connectionPool,
                  final Set<String> tablesAllowingAutoCreate,
                  final Set<String> tablesAllowingSchemaEvolution,
                  final DatabaseMetadata databaseMetadata,
                  final DbDialect dbDialect,
                  final int executionRetries) {
    this.executionRetries = executionRetries;
    ParameterValidator.notNull(connectionPool, "connectionPool");
    ParameterValidator.notNull(databaseMetadata, "databaseMetadata");
    ParameterValidator.notNull(tablesAllowingAutoCreate, "tablesAllowingAutoCreate");
    ParameterValidator.notNull(tablesAllowingSchemaEvolution, "tablesAllowingSchemaEvolution");
    ParameterValidator.notNull(dbDialect, "dbDialect");

    this.connectionPool = connectionPool;
    this.tablesAllowingAutoCreate = tablesAllowingAutoCreate;
    this.tablesAllowingSchemaEvolution = tablesAllowingSchemaEvolution;
    this.databaseMetadata = databaseMetadata;
    this.dbDialect = dbDialect;
  }


  public void update(final Map<String, Collection<SinkRecordField>> tablesToColumnsMap) throws SQLException {
    DatabaseMetadata.Changes changes = databaseMetadata.getChanges(tablesToColumnsMap);
    final Map<String, Collection<SinkRecordField>> amendmentsMap = changes.getAmendmentMap();
    final Map<String, Collection<SinkRecordField>> createMap = changes.getCreatedMap();
    //short-circuit if there is nothing to change
    if ((createMap == null || createMap.isEmpty()) && (amendmentsMap == null || amendmentsMap.isEmpty())) {
      return;
    }

    Connection connection = null;
    try {
      connection = connectionPool.getConnection();
      connection.setAutoCommit(false);

      createTables(createMap, connection);
      evolveTables(amendmentsMap, connection);

      connection.commit();
    } catch (RuntimeException ex) {
      connection.rollback();
      throw ex;
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  public void createTables(final Map<String, Collection<SinkRecordField>> tableMap,
                           final Connection connection) {
    if (tableMap == null || tableMap.size() == 0) {
      return;
    }

    for (final Map.Entry<String, Collection<SinkRecordField>> entry : tableMap.entrySet()) {
      final String tableName = entry.getKey();
      if (databaseMetadata.containsTable(tableName)) {
        continue;
      }
      if (!tablesAllowingAutoCreate.contains(tableName)) {
        throw new ConfigException(String.format("Table %s is not configured with auto-create", entry.getKey()));
      }

      boolean retry = true;
      int retryAttempts = executionRetries;
      while (retry) {
        try {
          final DbTable table = createTable(tableName, entry.getValue(), connection);
          databaseMetadata.update(table);
          retry = false;
        } catch (RuntimeException ex) {
          if (--retryAttempts <= 0) {
            //we want to stop the execution
            throw ex;
          }
          try {
            //should we exponentially wait?
            Thread.sleep(1000);
          } catch (InterruptedException e) {
          }
        }
      }
    }
  }

  private DbTable createTable(final String tableName,
                              final Collection<SinkRecordField> fields,
                              final Connection connection) {
    final String sql = dbDialect.getCreateQuery(tableName, fields);
    logger.info(String.format("Changing database structure for database %s%s%s",
            databaseMetadata.getDatabaseName(),
            System.lineSeparator(),
            sql));

    Statement statement = null;
    try {
      statement = connection.createStatement();
      statement.execute(sql);
      return DatabaseMetadata.getTableMetadata(connection, tableName);
    } catch (SQLException e) {
      logger.error("Creating table failed,", e);
      //tricky part work out if the table already exists
      try {
        if (DatabaseMetadata.tableExists(connection, tableName)) {
          final DbTable table = DatabaseMetadata.getTableMetadata(connection, tableName);
          //check for all fields from above where they are not present
          List<SinkRecordField> notPresentFields = null;
          for (final SinkRecordField f : fields) {
            if (!table.containsColumn(f.getName())) {
              if (notPresentFields == null) {
                notPresentFields = new ArrayList<>();
              }
              notPresentFields.add(f);
            }
          }
          //we have some difference; run amend table
          if (notPresentFields != null) {
            return amendTable(tableName, notPresentFields, connection);
          }
          return table;
        } else {
          //table doesn't exist throw; the above layer will pick up an retry
          throw new RuntimeException(e.getMessage(), e);
        }
      } catch (SQLException e1) {
        logger.error("There was an error on creating the table " + tableName + e1.getMessage(), e1);
        throw new RuntimeException(e1.getMessage(), e1);
      }
    } finally {
      try {
        if (statement != null) {
          statement.close();
        }
      } catch (SQLException e) {
        logger.error(e.getMessage(), e);
      }
    }

  }

  private void evolveTables(final Map<String, Collection<SinkRecordField>> tableMap,
                            final Connection connection) {
    if (tableMap == null || tableMap.size() == 0) {
      return;
    }
    for (final Map.Entry<String, Collection<SinkRecordField>> entry : tableMap.entrySet()) {
      final String tableName = entry.getKey();
      if (!databaseMetadata.containsTable(tableName)) {
        throw new RuntimeException(String.format("%s is set for amendments but hasn't been created yet", entry.getKey()));
      }
      if (!tablesAllowingSchemaEvolution.contains(entry.getKey())) {
        logger.warn(String.format("Table %s is not configured with schema evolution", entry.getKey()));
        continue;
      }

      boolean retry = true;
      int retryAttempts = executionRetries;
      while (retry) {
        try {
          final DbTable table = amendTable(tableName, entry.getValue(), connection);
          databaseMetadata.update(table);
          retry = false;
        } catch (RuntimeException ex) {
          if (--retryAttempts <= 0) {
            //we want to stop the execution
            throw ex;
          }
          try {
            //should we exponentially wait?
            Thread.sleep(1000);
          } catch (InterruptedException e) {
          }
        }
      }
    }
  }

  private DbTable amendTable(final String tableName,
                             final Collection<SinkRecordField> fields,
                             final Connection connection) {
    final List<String> amendTableQueries = dbDialect.getAlterTable(tableName, fields);

    Statement statement = null;
    try {
      connection.setAutoCommit(false);
      statement = connection.createStatement();

      for (String amendTableQuery : amendTableQueries) {
        logger.info(String.format("Changing database structure for database %s%s%s",
                databaseMetadata.getDatabaseName(),
                System.lineSeparator(),
                amendTableQuery));

        statement.execute(amendTableQuery);
      }
      //commit the transaction
      connection.commit();

      return DatabaseMetadata.getTableMetadata(connection, tableName);
    } catch (SQLException e) {
      //see if it there was a race with other tasks to add the colums
      try {
        final DbTable table = DatabaseMetadata.getTableMetadata(connection, tableName);
        List<SinkRecordField> notPresentFields = null;
        for (final SinkRecordField f : fields) {
          if (!table.containsColumn(f.getName())) {
            if (notPresentFields == null) {
              notPresentFields = new ArrayList<>();
            }
            notPresentFields.add(f);
          }
        }
        //we have some difference; run amend table
        if (notPresentFields != null) {
          return amendTable(tableName, notPresentFields, connection);
        }
        return table;
      } catch (SQLException e1) {
        throw new RuntimeException(e1.getMessage(), e);
      }
    } finally {
      try {
        if (statement != null) {
          statement.close();
        }
      } catch (SQLException e) {
        logger.error(e.getMessage(), e);
      }
    }
  }
}
