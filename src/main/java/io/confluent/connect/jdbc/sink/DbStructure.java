package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.confluent.connect.jdbc.sink.dialect.DbDialect;
import io.confluent.connect.jdbc.sink.metadata.DbTable;
import io.confluent.connect.jdbc.sink.metadata.DbTableColumn;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.sink.metadata.TableMetadataLoadingCache;

public class DbStructure {
  private final static Logger logger = LoggerFactory.getLogger(DbStructure.class);

  private final TableMetadataLoadingCache tableMetadataLoadingCache = new TableMetadataLoadingCache();

  private final DbDialect dbDialect;

  public DbStructure(DbDialect dbDialect) {
    this.dbDialect = dbDialect;
  }

  /**
   * @return whether a DDL operation was performed
   * @throws SQLException if a DDL operation was deemed necessary but failed
   */
  public boolean createOrAmendIfNecessary(
      final JdbcSinkConfig config,
      final Connection connection,
      final String tableName,
      final FieldsMetadata fieldsMetadata
  ) throws SQLException {
    if (tableMetadataLoadingCache.get(connection, tableName) == null) {
      try {
        create(config, connection, tableName, fieldsMetadata);
      } catch (SQLException sqle) {
        logger.warn("Create failed, will attempt amend if table already exists", sqle);
        if (DbMetadataQueries.tableExists(connection, tableName)) {
          tableMetadataLoadingCache.refresh(connection, tableName);
        } else {
          throw sqle;
        }
      }
    }
    return amendIfNecessary(config, connection, tableName, fieldsMetadata, config.maxRetries);
  }

  /**
   * @throws SQLException if CREATE failed
   */
  void create(
      final JdbcSinkConfig config,
      final Connection connection,
      final String tableName,
      final FieldsMetadata fieldsMetadata
  ) throws SQLException {
    if (!config.autoCreate) {
      throw new ConnectException(String.format("Table %s is missing and auto-creation is disabled", tableName));
    }
    final String sql = dbDialect.getCreateQuery(tableName, fieldsMetadata.allFields.values());
    logger.info("Creating table:{} with SQL: {}", tableName, sql);
    try (Statement statement = connection.createStatement()) {
      statement.executeUpdate(sql);
      connection.commit();
    }
    tableMetadataLoadingCache.refresh(connection, tableName);
  }

  /**
   * @return whether an ALTER was successfully performed
   * @throws SQLException if ALTER was deemed necessary but failed
   */
  boolean amendIfNecessary(
      final JdbcSinkConfig config,
      final Connection connection,
      final String tableName,
      final FieldsMetadata fieldsMetadata,
      final int maxRetries
  ) throws SQLException {
    // NOTE:
    //   The table might have extra columns defined (hopefully with default values), which is not a case we check for here.
    //   We also don't check if the data types for columns that do line-up are compatible.

    final DbTable tableMetadata = tableMetadataLoadingCache.get(connection, tableName);
    final Map<String, DbTableColumn> dbColumns = tableMetadata.columns;

// FIXME: SQLite JDBC driver seems to not always return the PK column names?
//    if (!tableMetadata.getPrimaryKeyColumnNames().equals(fieldsMetadata.keyFieldNames)) {
//      throw new ConnectException(String.format(
//          "Table %s has different primary key columns - database (%s), desired (%s)",
//          tableName, tableMetadata.getPrimaryKeyColumnNames(), fieldsMetadata.keyFieldNames
//      ));
//    }

    final Set<SinkRecordField> missingFields = missingFields(fieldsMetadata.allFields.values(), dbColumns.keySet());

    if (missingFields.isEmpty()) {
      return false;
    }

    if (!config.autoEvolve) {
      throw new ConnectException(String.format("Table %s is missing fields (%s) and auto-evolution is disabled", tableName, missingFields));
    }

    final List<String> amendTableQueries = dbDialect.getAlterTable(tableName, missingFields);
    logger.info("Amending table to add missing fields:{} maxRetries:{} with SQL: {}", missingFields, maxRetries, amendTableQueries);
    try (Statement statement = connection.createStatement()) {
      for (String amendTableQuery : amendTableQueries) {
        statement.executeUpdate(amendTableQuery);
      }
      connection.commit();
    } catch (SQLException sqle) {
      if (maxRetries <= 0) {
        throw new ConnectException(
            String.format("Failed to amend table '%s' to add missing fields: %s", tableName, missingFields),
            sqle
        );
      }
      logger.warn("Amend failed, re-attempting", sqle);
      tableMetadataLoadingCache.refresh(connection, tableName);
      // Perhaps there was a race with other tasks to add the columns
      return amendIfNecessary(
          config,
          connection,
          tableName,
          fieldsMetadata,
          maxRetries - 1
      );
    }

    tableMetadataLoadingCache.refresh(connection, tableName);
    return true;
  }

  Set<SinkRecordField> missingFields(Collection<SinkRecordField> fields, Set<String> dbColumnNames) {
    final Set<SinkRecordField> missingFields = new HashSet<>();
    for (SinkRecordField field : fields) {
      if (!dbColumnNames.contains(field.name)) {
        missingFields.add(field);
      }
    }
    return missingFields;
  }
}
