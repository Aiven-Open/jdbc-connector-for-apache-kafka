package io.confluent.connect.jdbc.sink.dialect;

import org.apache.kafka.connect.data.Schema;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.jdbc.sink.util.StringBuilderUtil;

import static io.confluent.connect.jdbc.sink.util.StringBuilderUtil.joinToBuilder;
import static io.confluent.connect.jdbc.sink.util.StringBuilderUtil.nCopiesToBuilder;
import static io.confluent.connect.jdbc.sink.util.StringBuilderUtil.stringSurroundTransform;

public class PostgreSqlDialect extends DbDialect {

  // The user is responsible for escaping the columns otherwise create table A and create table "A" is not the same

  public PostgreSqlDialect() {
    super(getSqlTypeMap(), "\"", "\"");
  }

  private static Map<Schema.Type, String> getSqlTypeMap() {
    Map<Schema.Type, String> map = new HashMap<>();
    map.put(Schema.Type.INT8, "SMALLINT");
    map.put(Schema.Type.INT16, "SMALLINT");
    map.put(Schema.Type.INT32, "INT");
    map.put(Schema.Type.INT64, "BIGINT");
    map.put(Schema.Type.FLOAT32, "FLOAT");
    map.put(Schema.Type.FLOAT64, "DOUBLE PRECISION");
    map.put(Schema.Type.BOOLEAN, "BOOLEAN");
    map.put(Schema.Type.STRING, "TEXT");
    map.put(Schema.Type.BYTES, "BYTEA");
    return map;
  }

  @Override
  public String getUpsertQuery(final String table, final Collection<String> keyCols, final Collection<String> cols) {
    final StringBuilder builder = new StringBuilder();
    builder.append("INSERT INTO ");
    builder.append(escapeTableName(table));
    builder.append(" (");
    joinToBuilder(builder, ",", keyCols, cols, stringSurroundTransform(escapeColumnNamesStart, escapeColumnNamesEnd));
    builder.append(") VALUES (");
    nCopiesToBuilder(builder, ",", "?", cols.size() + keyCols.size());
    builder.append(") ON CONFLICT (");
    joinToBuilder(builder, ",", keyCols, stringSurroundTransform(escapeColumnNamesStart, escapeColumnNamesEnd));
    builder.append(") DO UPDATE SET ");
    joinToBuilder(
        builder,
        ",",
        cols,
        new StringBuilderUtil.Transform<String>() {
          @Override
          public void apply(StringBuilder builder, String col) {
            builder.append(escapeColumnNamesStart);
            builder.append(col);
            builder.append(escapeColumnNamesEnd);
            builder.append("=EXCLUDED.");
            builder.append(escapeColumnNamesStart);
            builder.append(col);
            builder.append(escapeColumnNamesEnd);
          }
        }
    );
    return builder.toString();
  }
}
