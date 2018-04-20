/**
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
 **/

package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.NumericMapping;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class DataConverterTest {

  public static final BigDecimal BIG_DECIMAL = new BigDecimal(9.9);
  public static final long LONG = Long.MAX_VALUE;
  public static final int INT = Integer.MAX_VALUE;
  public static final short SHORT = Short.MAX_VALUE;
  public static final byte BYTE = Byte.MAX_VALUE;
  public static final double DOUBLE = Double.MAX_VALUE;

  @Parameterized.Parameters
  public static Iterable<Object[]> mapping() {
    return Arrays.asList(
        new Object[][] {
            // MAX_VALUE means this value doesn't matter
            // Parameter range 1-4
            { Type.BYTES, BIG_DECIMAL, NumericMapping.NONE, ResultSetMetaData.columnNoNulls, Types.NUMERIC, Integer.MAX_VALUE, 0 },
            { Type.BYTES, BIG_DECIMAL, NumericMapping.NONE, ResultSetMetaData.columnNoNulls, Types.NUMERIC, Integer.MAX_VALUE, -127 },
            { Type.BYTES, BIG_DECIMAL, NumericMapping.NONE, ResultSetMetaData.columnNullable, Types.NUMERIC, Integer.MAX_VALUE, 0 },
            { Type.BYTES, BIG_DECIMAL, NumericMapping.NONE, ResultSetMetaData.columnNullable, Types.NUMERIC, Integer.MAX_VALUE, -127 },

            // integers - non optional
            // Parameter range 5-8
            { Type.INT64, LONG, NumericMapping.PRECISION_ONLY, ResultSetMetaData.columnNoNulls, Types.NUMERIC, 18, 0 },
            { Type.INT32, INT, NumericMapping.PRECISION_ONLY, ResultSetMetaData.columnNoNulls, Types.NUMERIC, 8, 0, },
            { Type.INT16, SHORT, NumericMapping.PRECISION_ONLY, ResultSetMetaData.columnNoNulls, Types.NUMERIC, 3, 0, },
            { Type.INT8, BYTE, NumericMapping.PRECISION_ONLY, ResultSetMetaData.columnNoNulls, Types.NUMERIC, 1, 0, },

            // integers - optional
            // Parameter range 9-12
            { Type.INT64, LONG, NumericMapping.PRECISION_ONLY, ResultSetMetaData.columnNullable, Types.NUMERIC, 18, 0 },
            { Type.INT32, INT, NumericMapping.PRECISION_ONLY, ResultSetMetaData.columnNullable, Types.NUMERIC, 8, 0 },
            { Type.INT16, SHORT, NumericMapping.PRECISION_ONLY, ResultSetMetaData.columnNullable, Types.NUMERIC, 3, 0 },
            { Type.INT8, BYTE, NumericMapping.PRECISION_ONLY, ResultSetMetaData.columnNullable, Types.NUMERIC, 1, 0 },

            // scale != 0 - non optional
            // Parameter range 13-16
            { Type.BYTES, BIG_DECIMAL, NumericMapping.PRECISION_ONLY, ResultSetMetaData.columnNoNulls, Types.NUMERIC, 18, 1 },
            { Type.BYTES, BIG_DECIMAL, NumericMapping.PRECISION_ONLY, ResultSetMetaData.columnNoNulls, Types.NUMERIC, 8, 1 },
            { Type.BYTES, BIG_DECIMAL, NumericMapping.PRECISION_ONLY, ResultSetMetaData.columnNoNulls, Types.NUMERIC, 3, -1 },
            { Type.BYTES, BIG_DECIMAL, NumericMapping.PRECISION_ONLY, ResultSetMetaData.columnNoNulls, Types.NUMERIC, 1, -1 },

            // scale != 0 - optional
            // Parameter range 17-20
            { Type.BYTES, BIG_DECIMAL, NumericMapping.PRECISION_ONLY, ResultSetMetaData.columnNullable, Types.NUMERIC, 18, 1 },
            { Type.BYTES, BIG_DECIMAL, NumericMapping.PRECISION_ONLY, ResultSetMetaData.columnNullable, Types.NUMERIC, 8, 1 },
            { Type.BYTES, BIG_DECIMAL, NumericMapping.PRECISION_ONLY, ResultSetMetaData.columnNullable, Types.NUMERIC, 3, -1 },
            { Type.BYTES, BIG_DECIMAL, NumericMapping.PRECISION_ONLY, ResultSetMetaData.columnNullable, Types.NUMERIC, 1, -1 },

            // integers - non optional
            // Parameter range 21-25
            { Type.INT64, LONG, NumericMapping.BEST_FIT, ResultSetMetaData.columnNoNulls, Types.NUMERIC, 18, -1 },
            { Type.INT32, INT, NumericMapping.BEST_FIT, ResultSetMetaData.columnNoNulls, Types.NUMERIC, 8, -1 },
            { Type.INT16, SHORT, NumericMapping.BEST_FIT, ResultSetMetaData.columnNoNulls, Types.NUMERIC, 3, 0 },
            { Type.INT8, BYTE, NumericMapping.BEST_FIT, ResultSetMetaData.columnNoNulls, Types.NUMERIC, 1, 0 },
            { Type.BYTES, BIG_DECIMAL, NumericMapping.BEST_FIT, ResultSetMetaData.columnNoNulls, Types.NUMERIC, 19, -1 },

            // integers - optional
            // Parameter range 26-30
            { Type.INT64, LONG, NumericMapping.BEST_FIT, ResultSetMetaData.columnNullable, Types.NUMERIC, 18, -1 },
            { Type.INT32, INT, NumericMapping.BEST_FIT, ResultSetMetaData.columnNullable, Types.NUMERIC, 8, -1 },
            { Type.INT16, SHORT, NumericMapping.BEST_FIT, ResultSetMetaData.columnNullable, Types.NUMERIC, 3, 0 },
            { Type.INT8, BYTE, NumericMapping.BEST_FIT, ResultSetMetaData.columnNullable, Types.NUMERIC, 1, 0 },
            { Type.BYTES, BIG_DECIMAL, NumericMapping.BEST_FIT, ResultSetMetaData.columnNullable, Types.NUMERIC, 19, -1 },

            // floating point - fitting - non optional
            { Type.FLOAT64, DOUBLE, NumericMapping.BEST_FIT, ResultSetMetaData.columnNoNulls, Types.NUMERIC, 18, 127 },
            { Type.FLOAT64, DOUBLE, NumericMapping.BEST_FIT, ResultSetMetaData.columnNoNulls, Types.NUMERIC, 8, 1 },
            { Type.BYTES, BIG_DECIMAL, NumericMapping.BEST_FIT, ResultSetMetaData.columnNoNulls, Types.NUMERIC, 19, 1 },

            // floating point - fitting - optional
            { Type.FLOAT64, DOUBLE, NumericMapping.BEST_FIT, ResultSetMetaData.columnNullable, Types.NUMERIC, 18, 127 },
            { Type.FLOAT64, DOUBLE, NumericMapping.BEST_FIT, ResultSetMetaData.columnNullable, Types.NUMERIC, 8, 1 },
            { Type.BYTES, BIG_DECIMAL, NumericMapping.BEST_FIT, ResultSetMetaData.columnNullable, Types.NUMERIC, 19, 1 },
        }
    );
  }

  @Parameterized.Parameter(0)
  public Type expected;

  @Parameterized.Parameter(1)
  public Object expectedValue;

  @Parameterized.Parameter(2)
  public NumericMapping numMapping;

  @Parameterized.Parameter(3)
  public int optional;

  @Parameterized.Parameter(4)
  public int columnType;

  @Parameterized.Parameter(5)
  public int precision;

  @Parameterized.Parameter(6)
  public int scale;

  @Mock
  ResultSetMetaData metadata = mock(ResultSetMetaData.class);

  @Mock
  ResultSet resultSet = mock(ResultSet.class);

  @Test
  public void testSchemaConversionOnNumeric() throws Exception {
    when(metadata.getColumnCount()).thenReturn(1);
    when(metadata.getColumnType(1)).thenReturn(columnType);
    when(metadata.getColumnName(1)).thenReturn("PrimitiveField1");
    when(metadata.isNullable(1)).thenReturn(optional);
    when(metadata.getPrecision(1)).thenReturn(precision);
    when(metadata.getScale(1)).thenReturn(scale);

    Schema schema = DataConverter.convertSchema("foo", metadata, numMapping);
    List<Field> fields = schema.fields();
    assertEquals(metadata.getColumnCount(), fields.size());
    assertEquals(expected, fields.get(0).schema().type());
  }

  @Test
  public void testValueConversionOnNumeric() throws Exception {
    when(resultSet.getMetaData()).thenReturn(metadata);
    when(resultSet.getBigDecimal(1, scale)).thenReturn(BIG_DECIMAL);
    // scale is changed inside the DataConverter if it's equal to -127
    when(resultSet.getBigDecimal(1, -scale)).thenReturn(BIG_DECIMAL);
    when(resultSet.getLong(1)).thenReturn(LONG);
    when(resultSet.getInt(1)).thenReturn(INT);
    when(resultSet.getShort(1)).thenReturn(SHORT);
    when(resultSet.getByte(1)).thenReturn(BYTE);
    when(resultSet.getDouble(1)).thenReturn(DOUBLE);

    when(metadata.getColumnCount()).thenReturn(1);
    when(metadata.getColumnType(1)).thenReturn(columnType);
    when(metadata.getColumnName(1)).thenReturn("PrimitiveField1");
    when(metadata.getColumnLabel(1)).thenReturn("PrimitiveField1");
    when(metadata.isNullable(1)).thenReturn(optional);
    when(metadata.getPrecision(1)).thenReturn(precision);
    when(metadata.getScale(1)).thenReturn(scale);

    Schema schema = DataConverter.convertSchema("foo", metadata, numMapping);
    Struct record = DataConverter.convertRecord(schema, resultSet, numMapping);

    Object value = record.get("PrimitiveField1");
    assertEquals(expectedValue, value);
  }
}
