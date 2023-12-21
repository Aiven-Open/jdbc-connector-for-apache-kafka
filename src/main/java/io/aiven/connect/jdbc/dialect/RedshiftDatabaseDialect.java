package io.aiven.connect.jdbc.dialect;

import io.aiven.connect.jdbc.config.JdbcConfig;
import io.aiven.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.aiven.connect.jdbc.sink.metadata.SinkRecordField;
import io.aiven.connect.jdbc.util.ColumnId;
import io.aiven.connect.jdbc.util.ExpressionBuilder;
import io.aiven.connect.jdbc.util.TableDefinition;
import io.aiven.connect.jdbc.util.TableId;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

public class RedshiftDatabaseDialect extends PostgreSqlDatabaseDialect {
  private static final List<String> SINK_TABLE_TYPE_DEFAULT = List.of("TABLE");

  /**
   * The provider for {@link RedshiftDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(RedshiftDatabaseDialect.class.getSimpleName(), "redshift");
    }

    @Override
    public DatabaseDialect create(final JdbcConfig config) {
      return new RedshiftDatabaseDialect(config);
    }
  }

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public RedshiftDatabaseDialect(JdbcConfig config) {
    super(config);
  }

  @Override
  protected String getSqlType(final SinkRecordField field) {
    final String sqlType = getSqlTypeFromSchema(field.schema());
    return sqlType != null ? sqlType : super.getSqlType(field);
  }

  private String getSqlTypeFromSchema(final Schema schema) {
    if (schema.name() != null) {
      switch (schema.name()) {
        case Decimal.LOGICAL_NAME:
          return "DECIMAL";
        case Date.LOGICAL_NAME:
          return "DATE";
        case Time.LOGICAL_NAME:
          return "TIME";
        case Timestamp.LOGICAL_NAME:
          return "TIMESTAMP";
        default:
          // fall through to normal types
      }
    }
    switch (schema.type()) {
      case INT8:
      case INT16:
        return "SMALLINT";
      case INT32:
        return "INT";
      case INT64:
        return "BIGINT";
      case FLOAT32:
        return "REAL";
      case FLOAT64:
        return "DOUBLE PRECISION";
      case BOOLEAN:
        return "BOOLEAN";
      case STRING:
        return "VARCHAR(5000)";
      case BYTES:
        // return "VARBYTE";
        return "VARCHAR";
      case ARRAY:
        return getSqlTypeFromSchema(schema.valueSchema()) + "[]";
      default:
        return null;
    }
  }

  @Override
  protected boolean maybeBindPrimitive(PreparedStatement statement, int index, Schema schema, Object value)
      throws SQLException {
    if (schema.type() == Schema.Type.BYTES) {
      final byte[] bytes;
      if (value instanceof ByteBuffer) {
        final ByteBuffer buffer = ((ByteBuffer) value).slice();
        bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
      } else {
        bytes = (byte[]) value;
      }
      statement.setString(index, HexFormat.of().formatHex(bytes));
      return true;
    } else {
      return super.maybeBindPrimitive(statement, index, schema, value);
    }
  }

  @Override
  public String buildUpsertQueryStatement(final TableId table,
      final TableDefinition tableDefinition,
      final Collection<ColumnId> keyColumns,
      final Collection<ColumnId> nonKeyColumns) {

    final ExpressionBuilder.Transform<ColumnId> transformTargetSource = (builder, col) -> {
      builder.append("target.").appendIdentifier(col.name());
      builder.append("=source.").appendIdentifier(col.name());
    };

    final ExpressionBuilder.Transform<ColumnId> transformSource = (builder, col) ->
      builder.append("=source.").appendIdentifier(col.name());

    final ExpressionBuilder builder = expressionBuilder();
    builder.append("MERGE INTO ").append(table).append(" target");
    builder.append(" USING (").append("SELECT ")
        .appendList()
        .delimitedBy("? AS ").transformedBy(ExpressionBuilder.columnNames()).of(keyColumns, nonKeyColumns).append(")").append(" source");

    // Constructing the ON clause
    builder.append(" ON (");
    builder.appendList()
        .delimitedBy(" AND ")
        .transformedBy(transformTargetSource)
        .of(keyColumns);
    builder.append(")");

    // When Matched (Update)
    builder.append(" WHEN MATCHED THEN UPDATE SET ");
    builder.appendList()
        .delimitedBy(", ")
        .transformedBy(transformTargetSource)
        .of(nonKeyColumns);

    // When Not Matched (Insert)
    builder.append(" WHEN NOT MATCHED THEN INSERT (")
        .appendList()
        .delimitedBy(", ")
        .transformedBy(ExpressionBuilder.columnNames())
        .of(keyColumns, nonKeyColumns)
        .append(") VALUES (")
        .appendList()
        .delimitedBy(", ")
        .transformedBy(transformSource)
        .of(keyColumns, nonKeyColumns)
        .append(")");

    return builder.toString();
  }

  @Override
  protected List<String> getDefaultSinkTableTypes() {
    return SINK_TABLE_TYPE_DEFAULT;
  }
}