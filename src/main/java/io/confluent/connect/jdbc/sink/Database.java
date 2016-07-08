package io.confluent.connect.jdbc.sink;

import org.apache.kafka.common.config.ConfigException;
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

import io.confluent.connect.jdbc.sink.common.DatabaseMetadata;
import io.confluent.connect.jdbc.sink.common.DbTable;
import io.confluent.connect.jdbc.sink.common.ParameterValidator;
import io.confluent.connect.jdbc.sink.dialect.DbDialect;

/**
 * Controls the database changes - creating/amending tables.
 */
public class Database {
  private final static Logger logger = LoggerFactory.getLogger(Database.class);

  private final Set<String> tablesAllowingAutoCreate;
  private final Set<String> tablesAllowingSchemaEvolution;
  private final DatabaseMetadata databaseMetadata;
  private final DbDialect dbDialect;

  public Database(final Set<String> tablesAllowingAutoCreate,
                  final Set<String> tablesAllowingSchemaEvolution,
                  final DatabaseMetadata databaseMetadata,
                  final DbDialect dbDialect) {
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
  public void update(final Map<String, Collection<SinkRecordField>> tablesToColumnsMap, final Connection connection) throws SQLException {
    DatabaseMetadata.Changes changes = databaseMetadata.getChanges(tablesToColumnsMap);
    final Map<String, Collection<SinkRecordField>> amendmentsMap = changes.getAmendmentMap();
    final Map<String, Collection<SinkRecordField>> createMap = changes.getCreatedMap();
    if ((createMap == null || createMap.isEmpty()) && (amendmentsMap == null || amendmentsMap.isEmpty())) {
      //short-circuit if there is nothing to change
      return;
    }
    createTables(createMap, connection);
    evolveTables(amendmentsMap, connection);
  }

  /**
   * Create tables
   *
   * @param tableMap A map of table and sinkRecords for that table.
   * @param connection The database connection to use.
   */
  public void createTables(final Map<String, Collection<SinkRecordField>> tableMap, final Connection connection) throws SQLException {
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

      final DbTable table = createTable(tableName, entry.getValue(), connection);
      databaseMetadata.update(table);
    }
  }

  /**
   * Create a table
   *
   * @param tableName The table to create
   * @param fields The sinkRecord fields to use to create the columns
   * @param connection The database connection to use.
   */
  private DbTable createTable(final String tableName, final Collection<SinkRecordField> fields, final Connection connection) throws SQLException {
    final String sql = dbDialect.getCreateQuery(tableName, fields);

    logger.info("Changing database structure for database {}{}{}", databaseMetadata.getDatabaseName(), System.lineSeparator(), sql);

    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
      logger.info("Database structure changed database {}{}{}", databaseMetadata.getDatabaseName(), System.lineSeparator(), sql);
      return DatabaseMetadata.getTableMetadata(connection, tableName);
    } catch (SQLException e) {
      logger.error("Creating table={} failed", tableName, e);

      //tricky part work out if the table already exists
      if (DatabaseMetadata.tableExists(connection, tableName)) {
        final DbTable table = DatabaseMetadata.getTableMetadata(connection, tableName);
        //check for all fields from above where they are not present
        final List<SinkRecordField> notPresentFields = new ArrayList<>();
        for (final SinkRecordField f : fields) {
          if (!table.containsColumn(f.getName())) {
            notPresentFields.add(f);
          }
        }
        //we have some difference; run amend table
        if (!notPresentFields.isEmpty()) {
          return amendTable(tableName, notPresentFields, connection);
        }
        return table;
      } else {
        throw e;
      }
    }
  }

  /**
   * Evolve tables, add new columns
   *
   * @param tableMap A map of table and sinkRecords for that table.
   * @param connection The database connection to use.
   */
  private void evolveTables(final Map<String, Collection<SinkRecordField>> tableMap, final Connection connection) throws SQLException {
    if (tableMap == null || tableMap.isEmpty()) {
      return;
    }

    for (final Map.Entry<String, Collection<SinkRecordField>> entry : tableMap.entrySet()) {
      final String tableName = entry.getKey();

      if (!databaseMetadata.containsTable(tableName)) {
        throw new RuntimeException(String.format("%s is set for amendments but hasn't been created yet", entry.getKey()));
      }

      if (!tablesAllowingSchemaEvolution.contains(entry.getKey())) {
        logger.warn("Table {} is not configured with schema evolution", entry.getKey());
        continue;
      }

      final DbTable table = amendTable(tableName, entry.getValue(), connection);
      databaseMetadata.update(table);
    }
  }

  private DbTable amendTable(final String tableName, final Collection<SinkRecordField> fields, final Connection connection) throws SQLException {
    final List<String> amendTableQueries = dbDialect.getAlterTable(tableName, fields);
    connection.setAutoCommit(false);
    try (Statement statement = connection.createStatement()) {
      for (String amendTableQuery : amendTableQueries) {
        logger.info("Changing database structure for database {}{}{}", databaseMetadata.getDatabaseName(), System.lineSeparator(), amendTableQuery);
        statement.executeUpdate(amendTableQuery);
        connection.commit();
        logger.info("Database structure changed for database {}{}{}", databaseMetadata.getDatabaseName(), System.lineSeparator(), amendTableQuery);
      }
      return DatabaseMetadata.getTableMetadata(connection, tableName);
    } catch (SQLException e) {
      logger.error("Amending table={} failed", tableName, e);

      //see if it there was a race with other tasks to add the colums
      final DbTable table = DatabaseMetadata.getTableMetadata(connection, tableName);
      final List<SinkRecordField> notPresentFields = new ArrayList<>();
      for (final SinkRecordField f : fields) {
        if (!table.containsColumn(f.getName())) {
          notPresentFields.add(f);
        }
      }
      //we have some difference; run amend table
      if (!notPresentFields.isEmpty()) {
        // FIXME there should probably be a bound on max times we can recurse here?
        return amendTable(tableName, notPresentFields, connection);
      }
      return table;
    }
  }

}
