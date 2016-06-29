package io.confluent.connect.jdbc.sink.dialect;

import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.sink.SinkRecordField;
import io.confluent.connect.jdbc.sink.common.ParameterValidator;
import io.confluent.connect.jdbc.sink.common.StringBuilderUtil;

import static io.confluent.connect.jdbc.sink.common.StringBuilderUtil.joinToBuilder;
import static io.confluent.connect.jdbc.sink.common.StringBuilderUtil.stringSurroundTransform;

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
    ParameterValidator.notNullOrEmpty(tableName, "table");
    ParameterValidator.notNull(fields, "fields");
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("<fields> is empty.");
    }
    final StringBuilder builder = new StringBuilder("ALTER TABLE ");
    builder.append(handleTableName(tableName));
    builder.append(" ADD");

    boolean first = true;
    for (final SinkRecordField f : fields) {
      if (!first) {
        builder.append(",");
      } else {
        first = false;
      }
      builder.append(lineSeparator);
      builder.append(escapeColumnNamesStart).append(f.getName()).append(escapeColumnNamesEnd);
      builder.append(" ");
      builder.append(getSqlType(f.getType()));
      builder.append(" NULL");
    }
    //builder.append(";");
    final List<String> query = new ArrayList<String>(1);
    query.add(builder.toString());
    return query;
  }

  @Override
  public String getUpsertQuery(String table, List<String> cols, List<String> keyCols) {
    if (table == null || table.trim().length() == 0) {
      throw new IllegalArgumentException("<table> is not valid");
    }

    if (keyCols == null || keyCols.size() == 0) {
      throw new IllegalArgumentException("<keyColumns> is not valid. It has to be non null and non empty.");
    }

    final StringBuilder builder = new StringBuilder();
    builder.append("merge into ");
    String tableName = handleTableName(table);
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