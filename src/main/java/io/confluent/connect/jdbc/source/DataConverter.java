/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Types;

import io.confluent.connect.jdbc.util.DateTimeUtils;

/**
 * DataConverter handles translating table schemas to Kafka Connect schemas and row data to Kafka
 * Connect records.
 */
public class DataConverter {
  private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);

  public static Schema convertSchema(String tableName, ResultSetMetaData metadata, boolean mapNumerics)
      throws SQLException {
    // TODO: Detect changes to metadata, which will require schema updates
    SchemaBuilder builder = SchemaBuilder.struct().name(tableName);
    for (int col = 1; col <= metadata.getColumnCount(); col++) {
      addFieldSchema(metadata, col, builder, mapNumerics);
    }
    return builder.build();
  }

  public static Struct convertRecord(Schema schema, ResultSet resultSet, boolean mapNumerics)
      throws SQLException {
    ResultSetMetaData metadata = resultSet.getMetaData();
    Struct struct = new Struct(schema);
    for (int col = 1; col <= metadata.getColumnCount(); col++) {
      try {
        convertFieldValue(resultSet, col, metadata.getColumnType(col), struct,
                          metadata.getColumnLabel(col), mapNumerics);
      } catch (IOException e) {
        log.warn("Ignoring record because processing failed:", e);
      } catch (SQLException e) {
        log.warn("Ignoring record due to SQL error:", e);
      }
    }
    return struct;
  }


  private static void addFieldSchema(ResultSetMetaData metadata, int col,
                                     SchemaBuilder builder, boolean mapNumerics)
      throws SQLException {
    // Label is what the query requested the column name be using an "AS" clause, name is the
    // original
    String label = metadata.getColumnLabel(col);
    String name = metadata.getColumnName(col);
    String fieldName = label != null && !label.isEmpty() ? label : name;

    int sqlType = metadata.getColumnType(col);
    boolean optional = false;
    if (metadata.isNullable(col) == ResultSetMetaData.columnNullable ||
        metadata.isNullable(col) == ResultSetMetaData.columnNullableUnknown) {
      optional = true;
    }

    switch (sqlType) {
      case Types.NULL: {
        log.warn("JDBC type {} not currently supported", sqlType);
        break;
      }

      case Types.BOOLEAN: {
        if (optional) {
          builder.field(fieldName, Schema.OPTIONAL_BOOLEAN_SCHEMA);
        } else {
          builder.field(fieldName, Schema.BOOLEAN_SCHEMA);
        }
        break;
      }

      // ints <= 8 bits
      case Types.BIT: {
        if (optional) {
          builder.field(fieldName, Schema.OPTIONAL_INT8_SCHEMA);
        } else {
          builder.field(fieldName, Schema.INT8_SCHEMA);
        }
        break;
      }

      case Types.TINYINT: {
        if (optional) {
          if (metadata.isSigned(col)) {
            builder.field(fieldName, Schema.OPTIONAL_INT8_SCHEMA);
          } else {
            builder.field(fieldName, Schema.OPTIONAL_INT16_SCHEMA);
          }
        } else {
          if (metadata.isSigned(col)) {
            builder.field(fieldName, Schema.INT8_SCHEMA);
          } else {
            builder.field(fieldName, Schema.INT16_SCHEMA);
          }
        }
        break;
      }

      // 16 bit ints
      case Types.SMALLINT: {
        if (optional) {
          if (metadata.isSigned(col)) {
            builder.field(fieldName, Schema.OPTIONAL_INT16_SCHEMA);
          } else {
            builder.field(fieldName, Schema.OPTIONAL_INT32_SCHEMA);
          }
        } else {
          if (metadata.isSigned(col)) {
            builder.field(fieldName, Schema.INT16_SCHEMA);
          } else {
            builder.field(fieldName, Schema.INT32_SCHEMA);
          }
        }
        break;
      }

      // 32 bit ints
      case Types.INTEGER: {
        if (optional) {
          if (metadata.isSigned(col)) {
            builder.field(fieldName, Schema.OPTIONAL_INT32_SCHEMA);
          } else {
            builder.field(fieldName, Schema.OPTIONAL_INT64_SCHEMA);
          }
        } else {
          if (metadata.isSigned(col)) {
            builder.field(fieldName, Schema.INT32_SCHEMA);
          } else {
            builder.field(fieldName, Schema.INT64_SCHEMA);
          }
        }
        break;
      }

      // 64 bit ints
      case Types.BIGINT: {
        if (optional) {
          builder.field(fieldName, Schema.OPTIONAL_INT64_SCHEMA);
        } else {
          builder.field(fieldName, Schema.INT64_SCHEMA);
        }
        break;
      }

      // REAL is a single precision floating point value, i.e. a Java float
      case Types.REAL: {
        if (optional) {
          builder.field(fieldName, Schema.OPTIONAL_FLOAT32_SCHEMA);
        } else {
          builder.field(fieldName, Schema.FLOAT32_SCHEMA);
        }
        break;
      }

      // FLOAT is, confusingly, double precision and effectively the same as DOUBLE. See REAL
      // for single precision
      case Types.FLOAT:
      case Types.DOUBLE: {
        if (optional) {
          builder.field(fieldName, Schema.OPTIONAL_FLOAT64_SCHEMA);
        } else {
          builder.field(fieldName, Schema.FLOAT64_SCHEMA);
        }
        break;
      }

      case Types.NUMERIC:
        if (mapNumerics) {
          int precision = metadata.getPrecision(col);
          if (metadata.getScale(col) == 0 && precision < 19) { // integer
            Schema schema;
            if (precision > 9) {
              schema = (optional) ? Schema.OPTIONAL_INT64_SCHEMA :
                      Schema.INT64_SCHEMA;
            } else if (precision > 4) {
              schema = (optional) ? Schema.OPTIONAL_INT32_SCHEMA :
                      Schema.INT32_SCHEMA;
            } else if (precision > 2) {
              schema = (optional) ? Schema.OPTIONAL_INT16_SCHEMA :
                      Schema.INT16_SCHEMA;
            } else {
              schema = (optional) ? Schema.OPTIONAL_INT8_SCHEMA :
                      Schema.INT8_SCHEMA;
            }
            builder.field(fieldName, schema);
            break;
          }
        }

      case Types.DECIMAL: {
        int scale = metadata.getScale(col);
        if (scale == -127) //NUMBER without precision defined for OracleDB
          scale = 127;
        SchemaBuilder fieldBuilder = Decimal.builder(scale);
        if (optional) {
          fieldBuilder.optional();
        }
        builder.field(fieldName, fieldBuilder.build());
        break;
      }

      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.LONGNVARCHAR:
      case Types.CLOB:
      case Types.NCLOB:
      case Types.DATALINK:
      case Types.SQLXML: {
        // Some of these types will have fixed size, but we drop this from the schema conversion
        // since only fixed byte arrays can have a fixed size
        if (optional) {
          builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
        } else {
          builder.field(fieldName, Schema.STRING_SCHEMA);
        }
        break;
      }

      // Binary == fixed bytes
      // BLOB, VARBINARY, LONGVARBINARY == bytes
      case Types.BINARY:
      case Types.BLOB:
      case Types.VARBINARY:
      case Types.LONGVARBINARY: {
        if (optional) {
          builder.field(fieldName, Schema.OPTIONAL_BYTES_SCHEMA);
        } else {
          builder.field(fieldName, Schema.BYTES_SCHEMA);
        }
        break;
      }

      // Date is day + moth + year
      case Types.DATE: {
        SchemaBuilder dateSchemaBuilder = Date.builder();
        if (optional) {
          dateSchemaBuilder.optional();
        }
        builder.field(fieldName, dateSchemaBuilder.build());
        break;
      }

      // Time is a time of day -- hour, minute, seconds, nanoseconds
      case Types.TIME: {
        SchemaBuilder timeSchemaBuilder = Time.builder();
        if (optional) {
          timeSchemaBuilder.optional();
        }
        builder.field(fieldName, timeSchemaBuilder.build());
        break;
      }

      // Timestamp is a date + time
      case Types.TIMESTAMP: {
        SchemaBuilder tsSchemaBuilder = Timestamp.builder();
        if (optional) {
          tsSchemaBuilder.optional();
        }
        builder.field(fieldName, tsSchemaBuilder.build());
        break;
      }

      case Types.ARRAY:
      case Types.JAVA_OBJECT:
      case Types.OTHER:
      case Types.DISTINCT:
      case Types.STRUCT:
      case Types.REF:
      case Types.ROWID:
      default: {
        log.warn("JDBC type {} not currently supported", sqlType);
        break;
      }
    }
  }

  private static void convertFieldValue(ResultSet resultSet, int col, int colType,
                                        Struct struct, String fieldName, boolean mapNumerics)
      throws SQLException, IOException {
    final Object colValue;
    switch (colType) {
      case Types.NULL: {
        colValue = null;
        break;
      }

      case Types.BOOLEAN: {
        colValue = resultSet.getBoolean(col);
        break;
      }

      case Types.BIT: {
        /**
         * BIT should be either 0 or 1.
         * TODO: Postgres handles this differently, returning a string "t" or "f". See the
         * elasticsearch-jdbc plugin for an example of how this is handled
         */
        colValue = resultSet.getByte(col);
        break;
      }

      // 8 bits int
      case Types.TINYINT: {
        if (resultSet.getMetaData().isSigned(col)) {
          colValue = resultSet.getByte(col);
        } else {
          colValue = resultSet.getShort(col);
        }
        break;
      }

      // 16 bits int
      case Types.SMALLINT: {
        if (resultSet.getMetaData().isSigned(col)) {
          colValue = resultSet.getShort(col);
        } else {
          colValue = resultSet.getInt(col);
        }
        break;
      }

      // 32 bits int
      case Types.INTEGER: {
        if (resultSet.getMetaData().isSigned(col)) {
          colValue = resultSet.getInt(col);
        } else {
          colValue = resultSet.getLong(col);
        }
        break;
      }

      // 64 bits int
      case Types.BIGINT: {
        colValue = resultSet.getLong(col);
        break;
      }

      // REAL is a single precision floating point value, i.e. a Java float
      case Types.REAL: {
        colValue = resultSet.getFloat(col);
        break;
      }

      // FLOAT is, confusingly, double precision and effectively the same as DOUBLE. See REAL
      // for single precision
      case Types.FLOAT:
      case Types.DOUBLE: {
        colValue = resultSet.getDouble(col);
        break;
      }

      case Types.NUMERIC:
        if (mapNumerics) {
          ResultSetMetaData metadata = resultSet.getMetaData();
          int precision = metadata.getPrecision(col);
          if (metadata.getScale(col) == 0 && precision < 19) { // integer
            if (precision > 9) {
              colValue = resultSet.getLong(col);
            } else if (precision > 4) {
              colValue = resultSet.getInt(col);
            } else if (precision > 2) {
              colValue = resultSet.getShort(col);
            } else {
              colValue = resultSet.getByte(col);
            }
            break;
          }
        }
      case Types.DECIMAL: {
        ResultSetMetaData metadata = resultSet.getMetaData();
        int scale = metadata.getScale(col);
        if (scale == -127)
          scale = 127;
        colValue = resultSet.getBigDecimal(col, scale);
        break;
      }

      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR: {
        colValue = resultSet.getString(col);
        break;
      }

      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.LONGNVARCHAR: {
        colValue = resultSet.getNString(col);
        break;
      }

      // Binary == fixed, VARBINARY and LONGVARBINARY == bytes
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY: {
        colValue = resultSet.getBytes(col);
        break;
      }

      // Date is day + moth + year
      case Types.DATE: {
        colValue = resultSet.getDate(col, DateTimeUtils.UTC_CALENDAR.get());
        break;
      }

      // Time is a time of day -- hour, minute, seconds, nanoseconds
      case Types.TIME: {
        colValue = resultSet.getTime(col, DateTimeUtils.UTC_CALENDAR.get());
        break;
      }

      // Timestamp is a date + time
      case Types.TIMESTAMP: {
        colValue = resultSet.getTimestamp(col, DateTimeUtils.UTC_CALENDAR.get());
        break;
      }

      // Datalink is basically a URL -> string
      case Types.DATALINK: {
        URL url = resultSet.getURL(col);
        colValue = (url != null ? url.toString() : null);
        break;
      }

      // BLOB == fixed
      case Types.BLOB: {
        Blob blob = resultSet.getBlob(col);
        if (blob == null) {
          colValue = null;
        } else {
          if (blob.length() > Integer.MAX_VALUE) {
            throw new IOException("Can't process BLOBs longer than Integer.MAX_VALUE");
          }
          colValue = blob.getBytes(1, (int) blob.length());
          blob.free();
        }
        break;
      }
      case Types.CLOB:
      case Types.NCLOB: {
        Clob clob = (colType == Types.CLOB ? resultSet.getClob(col) : resultSet.getNClob(col));
        if (clob == null) {
          colValue = null;
        } else {
          if (clob.length() > Integer.MAX_VALUE) {
            throw new IOException("Can't process BLOBs longer than Integer.MAX_VALUE");
          }
          colValue = clob.getSubString(1, (int) clob.length());
          clob.free();
        }
        break;
      }

      // XML -> string
      case Types.SQLXML: {
        SQLXML xml = resultSet.getSQLXML(col);
        colValue = (xml != null ? xml.getString() : null);
        break;
      }

      case Types.ARRAY:
      case Types.JAVA_OBJECT:
      case Types.OTHER:
      case Types.DISTINCT:
      case Types.STRUCT:
      case Types.REF:
      case Types.ROWID:
      default: {
        // These are not currently supported, but we don't want to log something for every single
        // record we translate. There will already be errors logged for the schema translation
        return;
      }
    }

    // FIXME: Would passing in some extra info about the schema so we can get the Field by index
    // be faster than setting this by name?
    struct.put(fieldName, resultSet.wasNull() ? null : colValue);
  }

}
