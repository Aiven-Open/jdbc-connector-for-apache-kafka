package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.jdbc.common.DatabaseMetadata;
import com.datamountaineer.streamreactor.connect.jdbc.common.DbTable;
import com.datamountaineer.streamreactor.connect.jdbc.common.ParameterValidator;
import com.datamountaineer.streamreactor.connect.jdbc.dialect.DbDialect;
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

  public Database(final Set<String> tablesAllowingAutoCreate,
                  final Set<String> tablesAllowingSchemaEvolution,
                  final DatabaseMetadata databaseMetadata,
                  final DbDialect dbDialect,
                  final int executionRetries) {
    this.executionRetries = executionRetries;
    ParameterValidator.notNull(databaseMetadata, "databaseMetadata");
    ParameterValidator.notNull(tablesAllowingAutoCreate, "tablesAllowingAutoCreate");
    ParameterValidator.notNull(tablesAllowingSchemaEvolution, "tablesAllowingSchemaEvolution");
    ParameterValidator.notNull(dbDialect, "dbDialect");

    this.tablesAllowingAutoCreate = tablesAllowingAutoCreate;
    this.tablesAllowingSchemaEvolution = tablesAllowingSchemaEvolution;
    this.databaseMetadata = databaseMetadata;
    this.dbDialect = dbDialect;
  }

  /**
   * Apply any changes to the target table structures
   *
   * @param tablesToColumnsMap A map of table and sinkRecords for that table.
   */
  public void update(final Map<String, Collection<SinkRecordField>> tablesToColumnsMap,
                     final Connection connection) throws SQLException {
    DatabaseMetadata.Changes changes = databaseMetadata.getChanges(tablesToColumnsMap);
    final Map<String, Collection<SinkRecordField>> amendmentsMap = changes.getAmendmentMap();
    final Map<String, Collection<SinkRecordField>> createMap = changes.getCreatedMap();
    //short-circuit if there is nothing to change
    if ((createMap == null || createMap.isEmpty()) && (amendmentsMap == null || amendmentsMap.isEmpty())) {
      return;
    }

    createTables(createMap, connection);
    evolveTables(amendmentsMap, connection);
  }

  /**
   * Create tables
   *
   * @param tableMap   A map of table and sinkRecords for that table.
   * @param connection The database connection to use.
   */
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

  /**
   * Create a table
   *
   * @param tableName  The table to create
   * @param fields     The sinkRecord fields to use to create the columns
   * @param connection The database connection to use.
   */
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
      logger.info(String.format("Database structure changed database %s%s%s",
              databaseMetadata.getDatabaseName(),
              System.lineSeparator(),
              sql));
      return DatabaseMetadata.getTableMetadata(connection, tableName);
    } catch (SQLException e) {
      logger.error("Creating table failed." + e.getMessage(), e);
      SQLException inner = e.getNextException();
      while (inner != null) {
        logger.error(inner.getMessage(), inner);
        inner = e.getNextException();
      }
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

  /**
   * Evolve tables, add new columns
   *
   * @param tableMap   A map of table and sinkRecords for that table.
   * @param connection The database connection to use.
   */
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
            logger.error(ex.getMessage());
            throw ex;
          }
          try {
            //should we exponentially wait?
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
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

        statement.executeUpdate(amendTableQuery);
        //commit the transaction
        connection.commit();

        logger.info(String.format("Database structure changed for database %s%s%s",
                databaseMetadata.getDatabaseName(),
                System.lineSeparator(),
                amendTableQuery));
      }

      return DatabaseMetadata.getTableMetadata(connection, tableName);
    } catch (SQLException e) {
      logger.error("Amending database structure failed." + e.getMessage(), e);
      SQLException inner = e.getNextException();
      while (inner != null) {
        logger.error(inner.getMessage(), inner);
        inner = e.getNextException();
      }

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
