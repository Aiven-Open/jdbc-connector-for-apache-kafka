package io.confluent.connect.jdbc.sink.dialect;

import org.apache.kafka.connect.data.Schema;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;

import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.joinToBuilder;
import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.stringSurroundTransform;

public class SqlServerDialect extends DbDialect {

  public SqlServerDialect() {
    super(getSqlTypeMap(), "[", "]");
  }

  private static Map<Schema.Type, String> getSqlTypeMap() {
    Map<Schema.Type, String> map = new HashMap<>();
    map.put(Schema.Type.INT8, "tinyint");
    map.put(Schema.Type.INT16, "smallint");
    map.put(Schema.Type.INT32, "int");
    map.put(Schema.Type.INT64, "bigint");
    map.put(Schema.Type.FLOAT32, "real");
    map.put(Schema.Type.FLOAT64, "float");
    map.put(Schema.Type.BOOLEAN, "bit");
    map.put(Schema.Type.STRING, "varchar(256)");
    map.put(Schema.Type.BYTES, "varbinary(1024)");
    return map;
  }

  @Override
  public List<String> getAlterTable(String tableName, Collection<SinkRecordField> fields) {
    final StringBuilder builder = new StringBuilder("ALTER TABLE ");
    builder.append(escapeTableName(tableName));
    builder.append(" ADD");
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
          builder.append(" NOT NULL");
        }
      }
    });
    return Collections.singletonList(builder.toString());
  }

  @Override
  public String getUpsertQuery(String table, Collection<String> keyCols, Collection<String> cols) {
    final StringBuilder builder = new StringBuilder();
    builder.append("merge into ");
    String tableName = escapeTableName(table);
    builder.append(tableName);
    builder.append(" with (HOLDLOCK) AS target using (select ");
    joinToBuilder(builder, ", ", cols, keyCols, new StringBuilderUtil.Transform<String>() {
      @Override
      public void apply(StringBuilder builder, String col) {
        builder.append("? AS ").append(escapeColumnNamesStart).append(col).append(escapeColumnNamesEnd);
      }
    });
    builder.append(") AS incoming on (");
    joinToBuilder(builder, " and ", keyCols, new StringBuilderUtil.Transform<String>() {
      @Override
      public void apply(StringBuilder builder, String col) {
        builder.append("target.")
            .append(escapeColumnNamesStart).append(col).append(escapeColumnNamesEnd)
            .append("=incoming.").append(escapeColumnNamesStart).append(col).append(escapeColumnNamesEnd);
      }
    });
    builder.append(")");
    if (cols != null && cols.size() > 0) {
      builder.append(" when matched then update set ");
      joinToBuilder(builder, ",", cols, new StringBuilderUtil.Transform<String>() {
        @Override
        public void apply(StringBuilder builder, String col) {
          builder.append(escapeColumnNamesStart).append(col).append(escapeColumnNamesEnd)
              .append("=incoming.")
              .append(escapeColumnNamesStart).append(col).append(escapeColumnNamesEnd);
        }
      });
    }
    builder.append(" when not matched then insert (");
    joinToBuilder(builder, ", ", cols, keyCols, stringSurroundTransform(escapeColumnNamesStart, escapeColumnNamesEnd));
    builder.append(") values (");
    joinToBuilder(builder, ",", cols, keyCols, stringSurroundTransform("incoming." + escapeColumnNamesStart, escapeColumnNamesEnd));
    builder.append(");");
    return builder.toString();
  }
}