/*
 * Copyright 2016 Confluent Inc.
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

package io.confluent.connect.jdbc.sink.dialect;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.DateTimeUtils;

import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.Transform;
import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.joinToBuilder;
import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.nCopiesToBuilder;

public abstract class DbDialect {

  private final String escapeStart;
  private final String escapeEnd;

  DbDialect(String escapeStart, String escapeEnd) {
    this.escapeStart = escapeStart;
    this.escapeEnd = escapeEnd;
  }

  public final String getInsert(final String tableName, final Collection<String> keyColumns, final Collection<String> nonKeyColumns) {
    StringBuilder builder = new StringBuilder("INSERT INTO ");
    builder.append(escaped(tableName));
    builder.append("(");
    joinToBuilder(builder, ",", keyColumns, nonKeyColumns, escaper());
    builder.append(") VALUES(");
    nCopiesToBuilder(builder, ",", "?", keyColumns.size() + nonKeyColumns.size());
    builder.append(")");
    return builder.toString();
  }

  public String getUpsertQuery(final String table, final Collection<String> keyColumns, final Collection<String> columns) {
    throw new UnsupportedOperationException();
  }

  public String getCreateQuery(String tableName, Collection<SinkRecordField> fields) {
    final List<String> pkFieldNames = extractPrimaryKeyFieldNames(fields);
    final StringBuilder builder = new StringBuilder();
    builder.append("CREATE TABLE ");
    builder.append(escaped(tableName));
    builder.append(" (");
    writeColumnsSpec(builder, fields);
    if (!pkFieldNames.isEmpty()) {
      builder.append(",");
      builder.append(System.lineSeparator());
      builder.append("PRIMARY KEY(");
      joinToBuilder(builder, ",", pkFieldNames, escaper());
      builder.append(")");
    }
    builder.append(")");
    return builder.toString();
  }

  public List<String> getAlterTable(String tableName, Collection<SinkRecordField> fields) {
    final boolean newlines = fields.size() > 1;

    final StringBuilder builder = new StringBuilder("ALTER TABLE ");
    builder.append(escaped(tableName));
    builder.append(" ");
    joinToBuilder(builder, ",", fields, new Transform<SinkRecordField>() {
      @Override
      public void apply(StringBuilder builder, SinkRecordField f) {
        if (newlines) {
          builder.append(System.lineSeparator());
        }
        builder.append("ADD ");
        writeColumnSpec(builder, f);
      }
    });
    return Collections.singletonList(builder.toString());
  }

  protected void writeColumnsSpec(StringBuilder builder, Collection<SinkRecordField> fields) {
    joinToBuilder(builder, ",", fields, new Transform<SinkRecordField>() {
      @Override
      public void apply(StringBuilder builder, SinkRecordField f) {
        builder.append(System.lineSeparator());
        writeColumnSpec(builder, f);
      }
    });
  }

  protected void writeColumnSpec(StringBuilder builder, SinkRecordField f) {
    builder.append(escaped(f.name()));
    builder.append(" ");
    builder.append(getSqlType(f.schemaName(), f.schemaParameters(), f.schemaType()));
    if (f.defaultValue() != null) {
      builder.append(" DEFAULT ");
      formatColumnValue(builder, f.schemaName(), f.schemaParameters(), f.schemaType(), f.defaultValue());
    } else if (f.isOptional()) {
      builder.append(" NULL");
    } else {
      builder.append(" NOT NULL");
    }
  }

  protected void formatColumnValue(StringBuilder builder, String schemaName, Map<String, String> schemaParameters, Schema.Type type, Object value) {
    if (schemaName != null) {
      switch (schemaName) {
        case Decimal.LOGICAL_NAME:
          builder.append(value);
          return;
        case Date.LOGICAL_NAME:
          builder.append("'").append(DateTimeUtils.formatUtcDate((java.util.Date) value)).append("'");
          return;
        case Time.LOGICAL_NAME:
          builder.append("'").append(DateTimeUtils.formatUtcTime((java.util.Date) value)).append("'");
          return;
        case Timestamp.LOGICAL_NAME:
          builder.append("'").append(DateTimeUtils.formatUtcTimestamp((java.util.Date) value)).append("'");
          return;
      }
    }
    switch (type) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case FLOAT32:
      case FLOAT64:
        // no escaping required
        builder.append(value);
        break;
      case BOOLEAN:
        // 1 & 0 for boolean is more portable rather than TRUE/FALSE
        builder.append((Boolean) value ? '1' : '0');
        break;
      case STRING:
        builder.append("'").append(value).append("'");
        break;
      case BYTES:
        final byte[] bytes;
        if (value instanceof ByteBuffer) {
          final ByteBuffer buffer = ((ByteBuffer) value).slice();
          bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
        } else {
          bytes = (byte[]) value;
        }
        builder.append("x'").append(DatatypeConverter.printHexBinary(bytes)).append("'");
        break;
      default:
        throw new ConnectException("Unsupported type for column value: " + type);
    }
  }

  protected String getSqlType(String schemaName, Map<String, String> parameters, Schema.Type type) {
    throw new ConnectException(String.format("%s (%s) type doesn't have a mapping to the SQL database column type", schemaName, type));
  }

  protected String escaped(String identifier) {
    return escapeStart + identifier + escapeEnd;
  }

  protected Transform<String> escaper() {
    return new Transform<String>() {
      @Override
      public void apply(StringBuilder builder, String identifier) {
        builder.append(escapeStart).append(identifier).append(escapeEnd);
      }
    };
  }

  protected Transform<String> prefixedEscaper(final String prefix) {
    return new Transform<String>() {
      @Override
      public void apply(StringBuilder builder, String identifier) {
        builder.append(prefix).append(escapeStart).append(identifier).append(escapeEnd);
      }
    };
  }

  static List<String> extractPrimaryKeyFieldNames(Collection<SinkRecordField> fields) {
    final List<String> pks = new ArrayList<>();
    for (SinkRecordField f : fields) {
      if (f.isPrimaryKey()) {
        pks.add(f.name());
      }
    }
    return pks;
  }

  public static DbDialect fromConnectionString(final String url) {
    if (!url.startsWith("jdbc:")) {
      throw new ConnectException(String.format("Not a valid JDBC URL: %s", url));
    }

    if (url.startsWith("jdbc:sqlite:")) {
      // SQLite URL's are not in the format jdbc:protocol://FILE but jdbc:protocol:file
      return new SqliteDialect();
    }

    if (url.startsWith("jdbc:oracle:thin:@")) {
      return new OracleDialect();
    }

    if (url.startsWith("jdbc:sap")) {
      // HANA url's are in the format : jdbc:sap://$host:3(instance)(port)/
      return new HanaDialect();
    }

    final String protocol = extractProtocolFromUrl(url).toLowerCase();
    switch (protocol) {
      case "microsoft:sqlserver":
      case "sqlserver":
      case "jtds:sqlserver":
        return new SqlServerDialect();
      case "mariadb":
      case "mysql":
        return new MySqlDialect();
      case "postgresql":
        return new PostgreSqlDialect();
      default:
        return new GenericDialect();
    }
  }

  static String extractProtocolFromUrl(final String url) {
    if (!url.startsWith("jdbc:")) {
      throw new ConnectException(String.format("Not a valid JDBC URL: %s", url));
    }
    final int index = url.indexOf("://", "jdbc:".length());
    if (index < 0) {
      throw new ConnectException(String.format("Not a valid JDBC URL: %s", url));
    }
    return url.substring("jdbc:".length(), index);
  }
}