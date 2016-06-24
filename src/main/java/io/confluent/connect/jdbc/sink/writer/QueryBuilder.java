package io.confluent.connect.jdbc.sink.writer;

import java.util.List;

public interface QueryBuilder {
  String build(final String table, final List<String> nonKeyColumns, final List<String> keyColumns);
}
