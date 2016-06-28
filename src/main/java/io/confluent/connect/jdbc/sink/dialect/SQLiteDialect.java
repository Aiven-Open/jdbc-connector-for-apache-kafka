package io.confluent.connect.jdbc.sink.dialect;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.sink.SinkRecordField;
import io.confluent.connect.jdbc.sink.common.ParameterValidator;

/**
 * Provides SQL insert support for SQLite
 */
public class SQLiteDialect extends DbDialect {
  public SQLiteDialect() {
    super(getSqlTypeMap(), "`", "`");
  }

  private static Map<Schema.Type, String> getSqlTypeMap() {
    Map<Schema.Type, String> map = new HashMap<>();
    map.put(Schema.Type.INT8, "NUMERIC");
    map.put(Schema.Type.INT16, "NUMERIC");
    map.put(Schema.Type.INT32, "NUMERIC");
    map.put(Schema.Type.INT64, "NUMERIC");
    map.put(Schema.Type.FLOAT32, "REAL");
    map.put(Schema.Type.FLOAT64, "REAL");
    map.put(Schema.Type.BOOLEAN, "NUMERIC");
    map.put(Schema.Type.STRING, "TEXT");
    map.put(Schema.Type.BYTES, "BLOB");
    return map;
  }

  /**
   * Returns the query for creating a new table in the database
   *
   * @return The create query for the dialect
   */
  public String getCreateQuery(String tableName, Collection<SinkRecordField> fields) {
    ParameterValidator.notNull(fields, "fields");
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("<fields> is not valid.Not accepting empty collection of fields.");
    }
    final StringBuilder builder = new StringBuilder();
    builder.append(String.format("CREATE TABLE %s (", handleTableName(tableName)));
    boolean first = true;
    List<String> primaryKeys = new ArrayList<>();
    for (final SinkRecordField f : fields) {
      if (!first) {
        builder.append(",");
      } else {
        first = false;
      }
      builder.append(lineSeparator);
      builder.append(escapeColumnNamesStart + f.getName() + escapeColumnNamesEnd);
      builder.append(" ");
      builder.append(getSqlType(f.getType()));

      if (f.isPrimaryKey()) {
        builder.append(" NOT NULL ");
        primaryKeys.add(escapeColumnNamesStart + f.getName() + escapeColumnNamesEnd);
      } else {
        builder.append(" NULL");
      }
    }
    if (primaryKeys.size() > 0) {
      builder.append(",");
      builder.append(lineSeparator);
      builder.append("PRIMARY KEY(");
      builder.append(Joiner.on(",").join(primaryKeys));
      builder.append(")");
    }
    builder.append(");");
    return builder.toString();
  }

  @Override
  public List<String> getAlterTable(String tableName, Collection<SinkRecordField> fields) {
    ParameterValidator.notNullOrEmpty(tableName, "table");
    ParameterValidator.notNull(fields, "fields");
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("<fields> is empty.");
    }
    final List<String> queries = new ArrayList<>(fields.size());
    for (final SinkRecordField f : fields) {
      queries.add(String.format("ALTER TABLE %s ADD %s%s%s %s NULL;", handleTableName(tableName), escapeColumnNamesStart, f.getName(), escapeColumnNamesEnd,
                                getSqlType(f.getType())));
    }
    return queries;
  }

  @Override
  public String getUpsertQuery(String table, List<String> cols, List<String> keyCols) {
    if (table == null || table.trim().length() == 0) {
      throw new IllegalArgumentException("<table> is not a valid parameter");
    }
    if (cols == null || cols.size() == 0) {
      throw new IllegalArgumentException("<columns> is invalid.Expecting non null and non empty collection");
    }
    if (keyCols == null || keyCols.size() == 0) {
      throw new IllegalArgumentException(
          String.format("Your SQL table %s does not have any primary key/s. You can only UPSERT when your SQL table has primary key/s defined",
                        table)
      );
    }

    List<String> nonKeyColumns = new ArrayList<>(cols.size());
    for (String c : cols) {
      nonKeyColumns.add(escapeColumnNamesStart + c + escapeColumnNamesEnd);
    }

    List<String> keyColumns = new ArrayList<>(keyCols.size());
    for (String c : keyCols) {
      keyColumns.add(escapeColumnNamesStart + c + escapeColumnNamesEnd);
    }

    final String queryColumns = Joiner.on(",").join(Iterables.concat(nonKeyColumns, keyColumns));
    final String bindingValues = Joiner.on(",").join(Collections.nCopies(nonKeyColumns.size() + keyColumns.size(), "?"));

    final StringBuilder builder = new StringBuilder();
    builder.append(nonKeyColumns.get(0));
    builder.append("=?");
    for (int i = 1; i < nonKeyColumns.size(); ++i) {
      builder.append(",");
      builder.append(nonKeyColumns.get(i));
      builder.append("=?");
    }

    final StringBuilder whereBuilder = new StringBuilder();
    whereBuilder.append(keyColumns.get(0));
    whereBuilder.append("=?");
    for (int i = 1; i < keyColumns.size(); ++i) {
      whereBuilder.append(" and ")
          .append(keyCols.get(i)).append("=?");
    }

    return String.format("insert or ignore into %s(%s) values(%s)", handleTableName(table), queryColumns, bindingValues);
  }
}