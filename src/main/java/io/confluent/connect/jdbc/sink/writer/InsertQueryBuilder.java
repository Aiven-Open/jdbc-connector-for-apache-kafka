package io.confluent.connect.jdbc.sink.writer;

import io.confluent.connect.jdbc.sink.common.ParameterValidator;
import io.confluent.connect.jdbc.sink.dialect.DbDialect;

import java.util.List;

public final class InsertQueryBuilder implements QueryBuilder {

  private final DbDialect dialect;

  public InsertQueryBuilder(final DbDialect dialect) {

    ParameterValidator.notNull(dialect, "dialect");
    this.dialect = dialect;
  }

  @Override
  public String build(final String tableName,
                      final List<String> nonKeyColumns,
                      final List<String> keyColumns) {
    return dialect.getInsert(tableName, nonKeyColumns, keyColumns);
  }
}
