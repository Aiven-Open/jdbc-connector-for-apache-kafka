package io.confluent.connect.jdbc.sink.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.jdbc.sink.DbMetadataQueries;

public class TableMetadataLoadingCache {
  private static final Logger logger = LoggerFactory.getLogger(TableMetadataLoadingCache.class);

  private final Map<String, DbTable> cache = new HashMap<>();

  public DbTable get(final Connection connection, final String tableName) throws SQLException {
    DbTable dbTable = cache.get(tableName);
    if (dbTable == null) {
      if (DbMetadataQueries.tableExists(connection, tableName)) {
        dbTable = DbMetadataQueries.table(connection, tableName);
        cache.put(tableName, dbTable);
      } else {
        return null;
      }
    }
    return dbTable;
  }

  public DbTable refresh(final Connection connection, final String tableName) throws SQLException {
    DbTable dbTable = DbMetadataQueries.table(connection, tableName);
    logger.info("Updating cached metadata -- {}", dbTable);
    cache.put(dbTable.name, dbTable);
    return dbTable;
  }
}
