package io.confluent.connect.jdbc.sink.metadata;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DbTable {
  public final String name;
  public final Map<String, DbTableColumn> columns = new HashMap<>();
  public final Set<String> primaryKeyColumnNames = new HashSet<>();

  public DbTable(String name, List<DbTableColumn> columns) {
    this.name = name;
    for (final DbTableColumn column : columns) {
      this.columns.put(column.name, column);
      if (column.isPrimaryKey) {
        primaryKeyColumnNames.add(column.name);
      }
    }
  }

  @Override
  public String toString() {
    return "DbTable{" +
           "name='" + name + '\'' +
           ", columns=" + columns +
           '}';
  }
}