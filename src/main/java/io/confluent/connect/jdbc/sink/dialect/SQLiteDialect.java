package io.confluent.connect.jdbc.sink.dialect;

import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.sink.util.ParameterValidator;
import io.confluent.connect.jdbc.sink.util.StringBuilderUtil;

import static io.confluent.connect.jdbc.sink.util.StringBuilderUtil.joinToBuilder;
import static io.confluent.connect.jdbc.sink.util.StringBuilderUtil.nCopiesToBuilder;
import static io.confluent.connect.jdbc.sink.util.StringBuilderUtil.stringSurroundTransform;

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

    joinToBuilder(builder, ",", fields, new StringBuilderUtil.Transform<SinkRecordField>() {
      @Override
      public void apply(StringBuilder builder, SinkRecordField f) {
        builder.append(lineSeparator);
        builder.append(escapeColumnNamesStart).append(f.name).append(escapeColumnNamesEnd);
        builder.append(" ");
        builder.append(getSqlType(f.type));
        if (f.isOptional) {
          builder.append(" NULL");
        } else {
          builder.append(" NOT NULL ");
        }
      }
    });

    final List<String> pks = new ArrayList<>();
    for (SinkRecordField f : fields) {
      if (f.isPrimaryKey) {
        pks.add(f.name);
      }
    }

    if (!pks.isEmpty()) {
      builder.append(",");
      builder.append(lineSeparator);
      builder.append("PRIMARY KEY(");
      joinToBuilder(builder, ",", pks, stringSurroundTransform(escapeColumnNamesStart, escapeColumnNamesEnd));
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
      queries.add(String.format(
          "ALTER TABLE %s ADD %s%s%s %s %s",
          handleTableName(tableName),
          escapeColumnNamesStart, f.name, escapeColumnNamesEnd,
          getSqlType(f.type),
          f.isOptional ? "NULL" : "NOT NULL"
      ));
    }
    return queries;
  }

  @Override
  public String getUpsertQuery(String table, Collection<String> keyCols, Collection<String> cols) {
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
    StringBuilder builder = new StringBuilder();
    builder.append("insert or ignore into ");
    builder.append(handleTableName(table)).append("(");
    joinToBuilder(builder, ",", keyCols, cols, stringSurroundTransform(escapeColumnNamesStart, escapeColumnNamesEnd));
    builder.append(") values(");
    nCopiesToBuilder(builder, ",", "?", cols.size() + keyCols.size());
    builder.append(")");
    return builder.toString();
  }
}