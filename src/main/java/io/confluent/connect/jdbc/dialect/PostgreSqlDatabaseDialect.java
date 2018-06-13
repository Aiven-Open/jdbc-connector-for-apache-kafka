/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.jdbc.dialect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.source.ColumnMapping;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.ExpressionBuilder.Transform;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableId;

/**
 * A {@link DatabaseDialect} for PostgreSQL.
 */
public class PostgreSqlDatabaseDialect extends GenericDatabaseDialect {

  /**
   * The provider for {@link PostgreSqlDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(PostgreSqlDatabaseDialect.class.getSimpleName(), "postgresql");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new PostgreSqlDatabaseDialect(config);
    }
  }

  private static final String JSON_TYPE_NAME = "json";
  private static final String JSONB_TYPE_NAME = "jsonb";

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public PostgreSqlDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "\"", "\""));
  }

  /**
   * Perform any operations on a {@link PreparedStatement} before it is used. This is called from
   * the {@link #createPreparedStatement(Connection, String)} method after the statement is
   * created but before it is returned/used.
   *
   * <p>This method sets the {@link PreparedStatement#setFetchDirection(int) fetch direction}
   * to {@link ResultSet#FETCH_FORWARD forward} as an optimization for the driver to allow it to
   * scroll more efficiently through the result set and prevent out of memory errors.
   *
   * @param stmt the prepared statement; never null
   * @throws SQLException the error that might result from initialization
   */
  @Override
  protected void initializePreparedStatement(PreparedStatement stmt) throws SQLException {
    log.trace("Initializing PreparedStatement fetch direction to FETCH_FORWARD for '{}'", stmt);
    stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
  }


  @Override
  public String addFieldToSchema(
      ColumnDefinition columnDefn,
      SchemaBuilder builder
  ) {
    // Add the PostgreSQL-specific types first
    final String fieldName = fieldNameFor(columnDefn);
    switch (columnDefn.type()) {
      case Types.BIT: {
        // PostgreSQL allows variable length bit strings, but when length is 1 then the driver
        // returns a 't' or 'f' string value to represent the boolean value, so we need to handle
        // this as well as lengths larger than 8.
        boolean optional = columnDefn.isOptional();
        int numBits = columnDefn.precision();
        Schema schema;
        if (numBits <= 1) {
          schema = optional ? Schema.OPTIONAL_BOOLEAN_SCHEMA : Schema.BOOLEAN_SCHEMA;
        } else if (numBits <= 8) {
          // For consistency with what the connector did before ...
          schema = optional ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA;
        } else {
          schema = optional ? Schema.OPTIONAL_BYTES_SCHEMA : Schema.BYTES_SCHEMA;
        }
        builder.field(fieldName, schema);
        return fieldName;
      }
      case Types.OTHER: {
        // Some of these types will have fixed size, but we drop this from the schema conversion
        // since only fixed byte arrays can have a fixed size
        if (isJsonType(columnDefn)) {
          builder.field(
              fieldName,
              columnDefn.isOptional() ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA
          );
          return fieldName;
        }
        break;
      }
      default:
        break;
    }

    // Delegate for the remaining logic
    return super.addFieldToSchema(columnDefn, builder);
  }

  @Override
  public ColumnConverter createColumnConverter(
      ColumnMapping mapping
  ) {
    // First handle any PostgreSQL-specific types
    ColumnDefinition columnDefn = mapping.columnDefn();
    int col = mapping.columnNumber();
    switch (columnDefn.type()) {
      case Types.BIT: {
        // PostgreSQL allows variable length bit strings, but when length is 1 then the driver
        // returns a 't' or 'f' string value to represent the boolean value, so we need to handle
        // this as well as lengths larger than 8.
        final int numBits = columnDefn.precision();
        if (numBits <= 1) {
          return rs -> rs.getBoolean(col);
        } else if (numBits <= 8) {
          // Do this for consistency with earlier versions of the connector
          return rs -> rs.getByte(col);
        }
        return rs -> rs.getBytes(col);
      }
      case Types.OTHER: {
        if (isJsonType(columnDefn)) {
          return rs -> rs.getString(col);
        }
        break;
      }
      default:
        break;
    }

    // Delegate for the remaining logic
    return super.createColumnConverter(mapping);
  }

  protected boolean isJsonType(ColumnDefinition columnDefn) {
    String typeName = columnDefn.typeName();
    return JSON_TYPE_NAME.equalsIgnoreCase(typeName) || JSONB_TYPE_NAME.equalsIgnoreCase(typeName);
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
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
    switch (field.schemaType()) {
      case INT8:
        return "SMALLINT";
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
        return "TEXT";
      case BYTES:
        return "BLOB";
      default:
        return super.getSqlType(field);
    }
  }

  @Override
  public String buildUpsertQueryStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns
  ) {
    final Transform<ColumnId> transform = (builder, col) -> {
      builder.appendIdentifierQuoted(col.name())
             .append("=EXCLUDED.")
             .appendIdentifierQuoted(col.name());
    };

    ExpressionBuilder builder = expressionBuilder();
    builder.append("INSERT INTO ");
    builder.append(table);
    builder.append(" (");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(ExpressionBuilder.columnNames())
           .of(keyColumns, nonKeyColumns);
    builder.append(") VALUES (");
    builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
    builder.append(") ON CONFLICT (");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(ExpressionBuilder.columnNames())
           .of(keyColumns);
    builder.append(") DO UPDATE SET ");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(transform)
           .of(nonKeyColumns);
    return builder.toString();
  }

}
