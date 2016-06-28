package io.confluent.connect.jdbc.sink.writer;

import java.util.List;

import io.confluent.connect.jdbc.sink.dialect.DbDialect;

/**
 * Builds an UPSERT sql statement.
 */
public class UpsertQueryBuilder implements QueryBuilder {
  private final DbDialect dbDialect;

  public UpsertQueryBuilder(DbDialect dialect) {
    this.dbDialect = dialect;
  }

  /**
   * Builds the sql statement for an upsert
   *
   * @param table - The target table
   * @param nonKeyColumns - A list of columns in the target table which are not part of the primary key
   * @param keyColumns - A list of columns in the target table which make the primary key
   * @return An upsert for the dialect
   */
  @Override
  public String build(String table, List<String> nonKeyColumns, List<String> keyColumns) {
    return dbDialect.getUpsertQuery(table, nonKeyColumns, keyColumns);
  }


  public DbDialect getDbDialect() {
    return dbDialect;
  }
}
