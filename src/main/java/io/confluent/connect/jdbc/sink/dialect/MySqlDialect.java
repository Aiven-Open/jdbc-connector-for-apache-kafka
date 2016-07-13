package io.confluent.connect.jdbc.sink.dialect;

import org.apache.kafka.connect.data.Schema;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.jdbc.sink.util.StringBuilderUtil;

import static io.confluent.connect.jdbc.sink.util.StringBuilderUtil.joinToBuilder;
import static io.confluent.connect.jdbc.sink.util.StringBuilderUtil.nCopiesToBuilder;
import static io.confluent.connect.jdbc.sink.util.StringBuilderUtil.stringSurroundTransform;

public class MySqlDialect extends DbDialect {

  public MySqlDialect() {
    super(getSqlTypeMap(), "`", "`");
  }

  private static Map<Schema.Type, String> getSqlTypeMap() {
    Map<Schema.Type, String> map = new HashMap<>();
    map.put(Schema.Type.INT8, "TINYINT");
    map.put(Schema.Type.INT16, "SMALLINT");
    map.put(Schema.Type.INT32, "INT");
    map.put(Schema.Type.INT64, "BIGINT");
    map.put(Schema.Type.FLOAT32, "FLOAT");
    map.put(Schema.Type.FLOAT64, "DOUBLE");
    map.put(Schema.Type.BOOLEAN, "TINYINT");
    map.put(Schema.Type.STRING, "VARCHAR(256)");
    map.put(Schema.Type.BYTES, "VARBINARY(1024)");
    return map;
  }

  @Override
  public String getUpsertQuery(final String table, final Collection<String> keyCols, final Collection<String> cols) {
    if (table == null || table.trim().length() == 0) {
      throw new IllegalArgumentException("<table=> is not valid. A non null non empty string expected");
    }

    if (keyCols == null || keyCols.size() == 0) {
      throw new IllegalArgumentException(
          String.format("Your SQL table %s does not have any primary key/s. You can only UPSERT when your SQL table has primary key/s defined",
                        table));
    }

    //MySql doesn't support SQL 2003:merge so here how the upsert is handled

    final StringBuilder builder = new StringBuilder();
    builder.append("insert into ");
    builder.append(handleTableName(table));
    builder.append("(");
    joinToBuilder(builder, ",", keyCols, cols, stringSurroundTransform(escapeColumnNamesStart, escapeColumnNamesEnd));
    builder.append(") values(");
    nCopiesToBuilder(builder, ",", "?", cols.size() + keyCols.size());
    builder.append(") on duplicate key update ");
    joinToBuilder(
        builder,
        ",",
        cols,
        new StringBuilderUtil.Transform<String>() {
          @Override
          public void apply(StringBuilder builder, String col) {
            builder.append(escapeColumnNamesStart).append(col).append(escapeColumnNamesEnd)
                .append("=values(").append(escapeColumnNamesStart).append(col).append(escapeColumnNamesEnd).append(")");
          }
        }
    );
    return builder.toString();
  }
}
