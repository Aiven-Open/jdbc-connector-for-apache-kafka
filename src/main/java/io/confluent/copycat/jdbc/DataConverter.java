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

package io.confluent.copycat.jdbc;

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

import io.confluent.copycat.data.GenericRecord;
import io.confluent.copycat.data.GenericRecordBuilder;
import io.confluent.copycat.data.Schema;
import io.confluent.copycat.data.SchemaBuilder;

/**
 * DataConverter handles translating table schemas to Copycat schemas and row data to Copycat
 * records.
 */
public class DataConverter {
  private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);

  public static Schema convertSchema(String tableName, ResultSetMetaData metadata)
      throws SQLException {
    // TODO: Detect changes to metadata, which will require schema updates
    SchemaBuilder.RecordBuilder<Schema> builder = SchemaBuilder.record(tableName);
    SchemaBuilder.FieldAssembler<Schema> fields = builder.fields();
    for (int col = 1; col <= metadata.getColumnCount(); col++) {
      addFieldSchema(metadata, col, fields);
    }
    return fields.endRecord();
  }

  public static GenericRecord convertRecord(Schema schema, ResultSet resultSet)
      throws SQLException {
    ResultSetMetaData metadata = resultSet.getMetaData();
    GenericRecordBuilder builder = new GenericRecordBuilder(schema);
    for (int col = 1; col <= metadata.getColumnCount(); col++) {
      try {
        convertFieldValue(resultSet, col, metadata.getColumnType(col), builder,
                          metadata.getColumnLabel(col));
      } catch (IOException e) {
        log.warn("Ignoring record because processing failed:", e);
      } catch (SQLException e) {
        log.warn("Ignoring record due to SQL error:", e);
      }
    }
    return builder.build();
  }


  private static void addFieldSchema(ResultSetMetaData metadata, int col,
                                     SchemaBuilder.FieldAssembler<Schema> fields)
      throws SQLException {
    // Label is what the query requested the column name be using an "AS" clause, name is the
    // original
    String label = metadata.getColumnLabel(col);
    String name = metadata.getColumnName(col);
    String fieldName = label != null && !label.isEmpty() ? label : name;
    // Create the field for each type case to handle
    SchemaBuilder.FieldBuilder<Schema> builder = fields.name(fieldName);
    // FIXME builder.aliases() or builder.doc()?

    int sqlType = metadata.getColumnType(col);

    // We can check for nullable types at once here, but because of the DSL for schema builder
    // each block for handling a type needs to know whether it's been nulled. However, we can
    // reduce
    // TODO: This approach doesn't properly handle the case where the type isn't handled and we
    // don't setup the the field if the column is nullable since we'll start setting up the
    // nullable field but not finish.
    SchemaBuilder.BaseTypeBuilder<SchemaBuilder.UnionAccumulator<SchemaBuilder.NullDefault<Schema>>>
        nullableBuilder = null;
    if (metadata.isNullable(col) == ResultSetMetaData.columnNullable ||
        metadata.isNullable(col) == ResultSetMetaData.columnNullableUnknown) {
      // Setup the start of the nullable union
      nullableBuilder = builder.type().unionOf().nullType().and();
    }
    switch (sqlType) {
      case Types.NULL: {
        builder.type().nullType().noDefault();
        break;
      }

      case Types.BOOLEAN: {
        if (nullableBuilder != null) {
          nullableBuilder.booleanType().endUnion().noDefault();
        } else {
          builder.type().booleanType().noDefault();
        }
        break;
      }

      // ints <= 32 bits
      case Types.BIT:
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER: {
        if (nullableBuilder != null) {
          nullableBuilder.intType().endUnion().noDefault();
        } else {
          builder.type().intType().noDefault();
        }
        break;
      }

      // 64 bit ints
      case Types.BIGINT: {
        if (nullableBuilder != null) {
          nullableBuilder.longType().endUnion().noDefault();
        } else {
          builder.type().longType().noDefault();
        }
        break;
      }

      // REAL is a single precision floating point value, i.e. a Java float
      case Types.REAL: {
        if (nullableBuilder != null) {
          nullableBuilder.floatType().endUnion().noDefault();
        } else {
          builder.type().floatType().noDefault();
        }
        break;
      }

      // FLOAT is, confusingly, double precision and effectively the same as DOUBLE. See REAL
      // for single precision
      case Types.FLOAT:
      case Types.DOUBLE: {
        if (nullableBuilder != null) {
          nullableBuilder.doubleType().endUnion().noDefault();
        } else {
          builder.type().doubleType().noDefault();
        }
        break;
      }

      case Types.NUMERIC:
      case Types.DECIMAL: {
        // FIXME This should use Avro's decimal logical type
        log.warn("JDBC type {} not currently supported", sqlType);
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
        if (nullableBuilder != null) {
          nullableBuilder.stringType().endUnion().noDefault();
        } else {
          builder.type().stringType().noDefault();
        }
        break;
      }

      // Binary == fixed bytes
      case Types.BINARY: {
        int fixedSize = metadata.getPrecision(col);
        if (nullableBuilder != null) {
          nullableBuilder.fixed(fieldName).size(fixedSize).endUnion().noDefault();
        } else {
          builder.type().fixed(fieldName).size(fixedSize).noDefault();
        }
        break;
      }
      // BLOB, VARBINARY, LONGVARBINARY == bytes
      case Types.BLOB:
      case Types.VARBINARY:
      case Types.LONGVARBINARY: {
        if (nullableBuilder != null) {
          nullableBuilder.bytesType().endUnion().noDefault();
        } else {
          builder.type().bytesType().noDefault();
        }
        break;
      }

      // Date is day + moth + year
      case Types.DATE: {
        // FIXME Dates/times are hard
        log.warn("JDBC type DATE not currently supported");
        break;
      }

      // Time is a time of day -- hour, minute, seconds, nanoseconds
      case Types.TIME: {
        // FIXME Dates/times are hard
        log.warn("JDBC type TIME not currently supported");
        break;
      }

      // Timestamp is a date + time
      case Types.TIMESTAMP: {
        // FIXME Dates/times are hard
        log.warn("JDBC type TIMESTAMP not currently supported");
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
                                        GenericRecordBuilder builder, String fieldName)
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
        colValue = resultSet.getInt(col);
        break;
      }

      // ints <= 32 bits
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER: {
        colValue = resultSet.getInt(col);
        break;
      }

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
      case Types.DECIMAL: {
        // FIXME This should use Avro's decimal logical type
        // resultSet.getBigDecimal(col);
        log.warn("Skipping NUMERIC or DECIMAL field");
        return;
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
        // FIXME Dates/times are hard
        log.warn("Skipping DATE field");
        return;
      }

      // Time is a time of day -- hour, minute, seconds, nanoseconds
      case Types.TIME: {
        // FIXME Dates/times are hard
        log.warn("Skipping TIME field");
        return;
      }

      // Timestamp is a date + time
      case Types.TIMESTAMP: {
        // FIXME Dates/times are hard
        log.warn("Skipping TIMESTAMP field");
        return;
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
    builder.set(fieldName, colValue);
  }

}
