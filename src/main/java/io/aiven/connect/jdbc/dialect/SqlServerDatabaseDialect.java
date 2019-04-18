/*
 * Copyright 2019 Aiven Oy
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

package io.aiven.connect.jdbc.dialect;

import io.aiven.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.aiven.connect.jdbc.sink.metadata.SinkRecordField;
import io.aiven.connect.jdbc.util.*;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * A {@link DatabaseDialect} for SQL Server.
 */
public class SqlServerDatabaseDialect extends GenericDatabaseDialect {
  /**
   * The provider for {@link SqlServerDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(SqlServerDatabaseDialect.class.getSimpleName(), "microsoft:sqlserver", "sqlserver",
            "jtds:sqlserver");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new SqlServerDatabaseDialect(config);
    }
  }

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public SqlServerDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "[", "]"));
  }

  @Override
  protected boolean useCatalog() {
    // SQL Server uses JDBC's catalog to represent the database,
    // and JDBC's schema to represent the owner (e.g., "dbo")
    return true;
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
        case Decimal.LOGICAL_NAME:
          return "decimal(38," + field.schemaParameters().get(Decimal.SCALE_FIELD) + ")";
        case Date.LOGICAL_NAME:
          return "date";
        case Time.LOGICAL_NAME:
          return "time";
        case Timestamp.LOGICAL_NAME:
          return "datetime2";
        default:
          // pass through to normal types
      }
    }
    switch (field.schemaType()) {
      case INT8:
        return "tinyint";
      case INT16:
        return "smallint";
      case INT32:
        return "int";
      case INT64:
        return "bigint";
      case FLOAT32:
        return "real";
      case FLOAT64:
        return "float";
      case BOOLEAN:
        return "bit";
      case STRING:
        return "varchar(max)";
      case BYTES:
        return "varbinary(max)";
      default:
        return super.getSqlType(field);
    }
  }

  @Override
  public String buildDropTableStatement(
      TableId table,
      DropOptions options
  ) {
    ExpressionBuilder builder = expressionBuilder();

    if (options.ifExists()) {
      builder.append("IF OBJECT_ID('");
      builder.append(table);
      builder.append(", 'U') IS NOT NULL");
    }
    // SQL Server 2016 supports IF EXISTS
    builder.append("DROP TABLE ");
    builder.append(table);
    if (options.cascade()) {
      builder.append(" CASCADE");
    }
    return builder.toString();
  }

  @Override
  public List<String> buildAlterTable(
      TableId table,
      Collection<SinkRecordField> fields
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("ALTER TABLE ");
    builder.append(table);
    builder.append(" ADD");
    writeColumnsSpec(builder, fields);
    return Collections.singletonList(builder.toString());
  }

  @Override
  public String buildUpsertQueryStatement(
      TableId table,
      Collection<ColumnId> keyColumns,
      Collection<ColumnId> nonKeyColumns
  ) {
    ExpressionBuilder builder = expressionBuilder();
    builder.append("merge into ");
    builder.append(table);
    builder.append(" with (HOLDLOCK) AS target using (select ");
    builder.appendList()
           .delimitedBy(", ")
           .transformedBy(ExpressionBuilder.columnNamesWithPrefix("? AS "))
           .of(keyColumns, nonKeyColumns);
    builder.append(") AS incoming on (");
    builder.appendList()
           .delimitedBy(" and ")
           .transformedBy(this::transformAs)
           .of(keyColumns);
    builder.append(")");
    if (nonKeyColumns != null && !nonKeyColumns.isEmpty()) {
      builder.append(" when matched then update set ");
      builder.appendList()
             .delimitedBy(",")
             .transformedBy(this::transformUpdate)
             .of(nonKeyColumns);
    }
    builder.append(" when not matched then insert (");
    builder.appendList()
           .delimitedBy(", ")
           .transformedBy(ExpressionBuilder.columnNames())
           .of(nonKeyColumns, keyColumns);
    builder.append(") values (");
    builder.appendList()
           .delimitedBy(",")
           .transformedBy(ExpressionBuilder.columnNamesWithPrefix("incoming."))
           .of(nonKeyColumns, keyColumns);
    builder.append(");");
    return builder.toString();
  }

  @Override
  protected ColumnDefinition columnDefinition(
      ResultSet resultSet,
      ColumnId id,
      int jdbcType,
      String typeName,
      String classNameForType,
      ColumnDefinition.Nullability nullability,
      ColumnDefinition.Mutability mutability,
      int precision,
      int scale,
      Boolean signedNumbers,
      Integer displaySize,
      Boolean autoIncremented,
      Boolean caseSensitive,
      Boolean searchable,
      Boolean currency,
      Boolean isPrimaryKey
  ) {
    try {
      String isAutoIncremented = resultSet.getString(22);

      if ("yes".equalsIgnoreCase(isAutoIncremented)) {
        autoIncremented = Boolean.TRUE;
      } else if ("no".equalsIgnoreCase(isAutoIncremented)) {
        autoIncremented = Boolean.FALSE;
      }
    } catch (SQLException e) {
      log.warn("Unable to get auto incrementing column information", e);
    }

    return super.columnDefinition(
      resultSet,
      id,
      jdbcType,
      typeName,
      classNameForType,
      nullability,
      mutability,
      precision,
      scale,
      signedNumbers,
      displaySize,
      autoIncremented,
      caseSensitive,
      searchable,
      currency,
      isPrimaryKey
    );
  }

  private void transformAs(ExpressionBuilder builder, ColumnId col) {
    builder.append("target.")
           .appendIdentifierQuoted(col.name())
           .append("=incoming.")
           .appendIdentifierQuoted(col.name());
  }

  private void transformUpdate(ExpressionBuilder builder, ColumnId col) {
    builder.appendIdentifierQuoted(col.name())
           .append("=incoming.")
           .appendIdentifierQuoted(col.name());
  }

  @Override
  protected String sanitizedUrl(String url) {
    // SQL Server has semicolon delimited property name-value pairs, and several properties
    // that contain secrets
    return super.sanitizedUrl(url)
                .replaceAll("(?i)(;password=)[^;]*", "$1****")
                .replaceAll("(?i)(;keyStoreSecret=)[^;]*", "$1****")
                .replaceAll("(?i)(;gsscredential=)[^;]*", "$1****");
  }
}
