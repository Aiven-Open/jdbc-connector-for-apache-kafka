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
import static io.confluent.connect.jdbc.sink.util.StringBuilderUtil.stringSurroundTransform;

public class OracleDialect extends DbDialect {
  public OracleDialect() {
    super(getSqlTypeMap(), "\"", "\"");
  }

  private static Map<Schema.Type, String> getSqlTypeMap() {
    Map<Schema.Type, String> map = new HashMap<>();
    map.put(Schema.Type.INT8, "TINYINT");
    map.put(Schema.Type.INT16, "SMALLINT");
    map.put(Schema.Type.INT32, "INTEGER");
    map.put(Schema.Type.INT64, "NUMBER(19)");
    map.put(Schema.Type.FLOAT32, "REAL");
    map.put(Schema.Type.FLOAT64, "BINARY_DOUBLE");
    map.put(Schema.Type.BOOLEAN, "NUMBER(1,0)");
    map.put(Schema.Type.STRING, "VARCHAR(256)");
    map.put(Schema.Type.BYTES, "BLOB");
    return map;
  }

  @Override
  public List<String> getAlterTable(String tableName, Collection<SinkRecordField> fields) {
    ParameterValidator.notNullOrEmpty(tableName, "table");
    ParameterValidator.notNull(fields, "fields");
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("<fields> is empty.");
    }
    final StringBuilder builder = new StringBuilder("ALTER TABLE ");
    builder.append(handleTableName(tableName)); //yes oracles needs it uppercase
    builder.append(" ADD(");

    joinToBuilder(
        builder,
        ",",
        fields,
        new StringBuilderUtil.Transform<SinkRecordField>() {
          @Override
          public void apply(StringBuilder builder, SinkRecordField f) {
            builder.append(lineSeparator);
            builder.append(escapeColumnNamesStart)
                .append(f.name)
                .append(escapeColumnNamesEnd);
            builder.append(" ");
            builder.append(getSqlType(f.type));
            if (f.isOptional) {
              builder.append(" NULL");
            } else {
              builder.append(" NOT NULL");
            }
          }
        }
    );

    builder.append(")");

    final List<String> query = new ArrayList<String>(1);
    query.add(builder.toString());
    return query;
  }

  @Override
  public String getUpsertQuery(final String table, Collection<String> keyCols, Collection<String> cols) {
    if (table == null || table.trim().length() == 0) {
      throw new IllegalArgumentException("<table> is not valid");
    }

    if (keyCols == null || keyCols.size() == 0) {
      throw new IllegalArgumentException("<keyColumns> is not valid. It has to be non null and non empty.");
    }

    // https://blogs.oracle.com/cmar/entry/using_merge_to_do_an

    final StringBuilder builder = new StringBuilder();
    builder.append("merge into ");
    final String tableName = handleTableName(table);
    builder.append(tableName);
    builder.append(" using (select ");
    joinToBuilder(builder, ", ", keyCols, cols, stringSurroundTransform("? " + escapeColumnNamesStart, escapeColumnNamesEnd));
    builder.append(" FROM dual) incoming on(");
    joinToBuilder(builder, " and ", keyCols, new StringBuilderUtil.Transform<String>() {
      @Override
      public void apply(StringBuilder builder, String col) {
        builder.append(tableName).append(".")
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
          builder.append(tableName).append(".")
              .append(escapeColumnNamesStart).append(col).append(escapeColumnNamesEnd)
              .append("=incoming.").append(escapeColumnNamesStart).append(col).append(escapeColumnNamesEnd);
        }
      });
    }

    builder.append(" when not matched then insert(");
    joinToBuilder(builder, ",", cols, keyCols, stringSurroundTransform(tableName + "." + escapeColumnNamesStart, escapeColumnNamesEnd));
    builder.append(") values(");
    joinToBuilder(builder, ",", cols, keyCols, stringSurroundTransform("incoming." + escapeColumnNamesStart, escapeColumnNamesEnd));
    builder.append(")");
    return builder.toString();
  }
}
