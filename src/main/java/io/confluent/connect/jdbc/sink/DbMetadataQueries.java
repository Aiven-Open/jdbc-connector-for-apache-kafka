package io.confluent.connect.jdbc.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.confluent.connect.jdbc.sink.metadata.DbTable;
import io.confluent.connect.jdbc.sink.metadata.DbTableColumn;

public abstract class DbMetadataQueries {
  private static final Logger logger = LoggerFactory.getLogger(DbMetadataQueries.class);

  public static boolean tableExists(final Connection connection, final String tableName) throws SQLException {
    final String catalog = connection.getCatalog();

    final DatabaseMetaData meta = connection.getMetaData();

    final String product = meta.getDatabaseProductName();
    final String schema = getSchema(connection, product);

    logger.info("Checking table:{} exists for product:{} schema:{} catalog:", tableName, product, schema, catalog);

    try (ResultSet rs = meta.getTables(catalog, schema, tableName, new String[]{"TABLE"})) {
      final boolean exists = rs.next();
      logger.info("product:{} schema:{} catalog:{} -- table:{} is {}", product, schema, catalog, tableName, exists ? "present" : "absent");
      return exists;
    }
  }

  public static Map<String, DbTable> allTables(final Connection connection) throws SQLException {
    final Map<String, DbTable> tables = new HashMap<>();
    final String catalog = connection.getCatalog();
    final DatabaseMetaData dbMetadata = connection.getMetaData();
    final ResultSet tablesRs = dbMetadata.getTables(catalog, null, null, new String[]{"TABLE"});
    while (tablesRs.next()) {
      final String tableName = tablesRs.getString("TABLE_NAME");
      final List<DbTableColumn> columns = DbMetadataQueries.columns(connection, tableName);
      tables.put(tableName, new DbTable(tableName, columns));
    }
    return tables;
  }

  public static DbTable table(final Connection connection, final String tableName) throws SQLException {
    return new DbTable(tableName, columns(connection, tableName));
  }

  public static List<DbTableColumn> columns(final Connection connection, final String tableName) throws SQLException {
    final DatabaseMetaData dbMetaData = connection.getMetaData();
    final String product = dbMetaData.getDatabaseProductName();
    final String catalog = connection.getCatalog();

    final String schema = getSchema(connection, product);
    final String tableNameForQuery = product.equalsIgnoreCase("oracle") ? tableName.toUpperCase() : tableName;

    logger.info("Querying column metadata for product:{} schema:{} catalog:{} table:{}", product, schema, catalog, tableNameForQuery);

    final Set<String> pkColumns = new HashSet<>();
    try (final ResultSet primaryKeysResultSet = dbMetaData.getPrimaryKeys(catalog, schema, tableNameForQuery)) {
      while (primaryKeysResultSet.next()) {
        final String colName = primaryKeysResultSet.getString("COLUMN_NAME");
        pkColumns.add(colName);
      }
    }

    final List<DbTableColumn> columns = new ArrayList<>();
    try (final ResultSet columnsResultSet = dbMetaData.getColumns(catalog, schema, tableNameForQuery, null)) {
      while (columnsResultSet.next()) {
        final String colName = columnsResultSet.getString("COLUMN_NAME");
        final int sqlType = columnsResultSet.getInt("DATA_TYPE");
        final boolean isPk = pkColumns.contains(colName);
        final boolean isNullable = !isPk // SQLite can report PK's as nullable
                                   && Objects.equals("YES", columnsResultSet.getString("IS_NULLABLE"));
        columns.add(new DbTableColumn(colName, isPk, isNullable, sqlType));
      }
    }
    return columns;
  }

  private static String getSchema(final Connection connection, final String product) throws SQLException {
    if (product.equalsIgnoreCase("oracle")) {
      // Use SQL to retrieve the database name for Oracle, apparently the JDBC API doesn't work as expected
      try (
          Statement statement = connection.createStatement();
          ResultSet rs = statement.executeQuery("select sys_context('userenv','current_schema') x from dual")
      ) {
        if (rs.next()) {
          return rs.getString("x").toUpperCase();
        } else {
          throw new SQLException("Failed to determine Oracle schema");
        }
      }
    } else if (product.toLowerCase().startsWith("postgre")) {
      return connection.getSchema();
    } else {
      return null;
    }
  }
}