/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2017 Confluent Inc.
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

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.aiven.connect.jdbc.config.JdbcConfig;
import io.aiven.connect.jdbc.source.ColumnMapping;
import io.aiven.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.aiven.connect.jdbc.util.ColumnDefinition;
import io.aiven.connect.jdbc.util.ColumnId;
import io.aiven.connect.jdbc.util.TableId;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class GenericDatabaseDialectTypeTest {

    public static final boolean NULLABLE = true;
    public static final boolean NOT_NULLABLE = false;
    public static final TableId TABLE_ID = new TableId(null, null, "MyTable");
    public static final ColumnId COLUMN_ID = new ColumnId(TABLE_ID, "columnA", "aliasA");
    public static final BigDecimal BIG_DECIMAL = new BigDecimal("9.9");
    public static final long LONG = Long.MAX_VALUE;
    public static final int INT = Integer.MAX_VALUE;
    public static final short SHORT = Short.MAX_VALUE;
    public static final byte BYTE = Byte.MAX_VALUE;
    public static final double DOUBLE = Double.MAX_VALUE;
    protected boolean signed = true;
    protected GenericDatabaseDialect dialect;
    protected SchemaBuilder schemaBuilder;
    protected DatabaseDialect.ColumnConverter converter;
    @Mock
    ResultSet resultSet = mock(ResultSet.class);
    @Mock
    ColumnDefinition columnDefn = mock(ColumnDefinition.class);

    public static Stream<Object[]> data() {
        return Arrays.stream(
            new Object[][]{
                // MAX_VALUE means this value doesn't matter
                // Parameter range 1-4
                {Schema.Type.BYTES, BIG_DECIMAL,
                    JdbcSourceConnectorConfig.NumericMapping.NONE, NOT_NULLABLE, Types.NUMERIC, Integer.MAX_VALUE, 0},
                {Schema.Type.BYTES, BIG_DECIMAL,
                    JdbcSourceConnectorConfig.NumericMapping.NONE, NULLABLE, Types.NUMERIC, Integer.MAX_VALUE, -127},
                {Schema.Type.BYTES, BIG_DECIMAL,
                    JdbcSourceConnectorConfig.NumericMapping.NONE, NULLABLE, Types.NUMERIC, Integer.MAX_VALUE, 0},
                {Schema.Type.BYTES, BIG_DECIMAL,
                    JdbcSourceConnectorConfig.NumericMapping.NONE, NULLABLE, Types.NUMERIC, Integer.MAX_VALUE, -127},

                // integers - non optional
                // Parameter range 5-8
                {Schema.Type.INT64, LONG,
                    JdbcSourceConnectorConfig.NumericMapping.PRECISION_ONLY, NOT_NULLABLE, Types.NUMERIC, 18, 0},
                {Schema.Type.INT32, INT,
                    JdbcSourceConnectorConfig.NumericMapping.PRECISION_ONLY, NOT_NULLABLE, Types.NUMERIC, 8, 0},
                {Schema.Type.INT16, SHORT,
                    JdbcSourceConnectorConfig.NumericMapping.PRECISION_ONLY, NOT_NULLABLE, Types.NUMERIC, 3, 0},
                {Schema.Type.INT8, BYTE,
                    JdbcSourceConnectorConfig.NumericMapping.PRECISION_ONLY, NOT_NULLABLE, Types.NUMERIC, 1, 0},

                // integers - optional
                // Parameter range 9-12
                {Schema.Type.INT64, LONG,
                    JdbcSourceConnectorConfig.NumericMapping.PRECISION_ONLY, NULLABLE, Types.NUMERIC, 18, 0},
                {Schema.Type.INT32, INT,
                    JdbcSourceConnectorConfig.NumericMapping.PRECISION_ONLY, NULLABLE, Types.NUMERIC, 8, 0},
                {Schema.Type.INT16, SHORT,
                    JdbcSourceConnectorConfig.NumericMapping.PRECISION_ONLY, NULLABLE, Types.NUMERIC, 3, 0},
                {Schema.Type.INT8, BYTE,
                    JdbcSourceConnectorConfig.NumericMapping.PRECISION_ONLY, NULLABLE, Types.NUMERIC, 1, 0},

                // scale != 0 - non optional
                // Parameter range 13-16
                {Schema.Type.BYTES, BIG_DECIMAL,
                    JdbcSourceConnectorConfig.NumericMapping.PRECISION_ONLY, NOT_NULLABLE, Types.NUMERIC, 18, 1},
                {Schema.Type.BYTES, BIG_DECIMAL,
                    JdbcSourceConnectorConfig.NumericMapping.PRECISION_ONLY, NOT_NULLABLE, Types.NUMERIC, 8, 1},
                {Schema.Type.BYTES, BIG_DECIMAL,
                    JdbcSourceConnectorConfig.NumericMapping.PRECISION_ONLY, NOT_NULLABLE, Types.NUMERIC, 3, -1},
                {Schema.Type.BYTES, BIG_DECIMAL,
                    JdbcSourceConnectorConfig.NumericMapping.PRECISION_ONLY, NOT_NULLABLE, Types.NUMERIC, 1, -1},

                // scale != 0 - optional
                // Parameter range 17-20
                {Schema.Type.BYTES, BIG_DECIMAL,
                    JdbcSourceConnectorConfig.NumericMapping.PRECISION_ONLY, NULLABLE, Types.NUMERIC, 18, 1},
                {Schema.Type.BYTES, BIG_DECIMAL,
                    JdbcSourceConnectorConfig.NumericMapping.PRECISION_ONLY, NULLABLE, Types.NUMERIC, 8, 1},
                {Schema.Type.BYTES, BIG_DECIMAL,
                    JdbcSourceConnectorConfig.NumericMapping.PRECISION_ONLY, NULLABLE, Types.NUMERIC, 3, -1},
                {Schema.Type.BYTES, BIG_DECIMAL,
                    JdbcSourceConnectorConfig.NumericMapping.PRECISION_ONLY, NULLABLE, Types.NUMERIC, 1, -1},

                // integers - non optional
                // Parameter range 21-25
                {Schema.Type.INT64, LONG,
                    JdbcSourceConnectorConfig.NumericMapping.BEST_FIT, NOT_NULLABLE, Types.NUMERIC, 18, -1},
                {Schema.Type.INT32, INT,
                    JdbcSourceConnectorConfig.NumericMapping.BEST_FIT, NOT_NULLABLE, Types.NUMERIC, 8, -1},
                {Schema.Type.INT16, SHORT,
                    JdbcSourceConnectorConfig.NumericMapping.BEST_FIT, NOT_NULLABLE, Types.NUMERIC, 3, 0},
                {Schema.Type.INT8, BYTE,
                    JdbcSourceConnectorConfig.NumericMapping.BEST_FIT, NOT_NULLABLE, Types.NUMERIC, 1, 0},
                {Schema.Type.BYTES, BIG_DECIMAL,
                    JdbcSourceConnectorConfig.NumericMapping.BEST_FIT, NOT_NULLABLE, Types.NUMERIC, 19, -1},

                // integers - optional
                // Parameter range 26-30
                {Schema.Type.INT64, LONG,
                    JdbcSourceConnectorConfig.NumericMapping.BEST_FIT, NULLABLE, Types.NUMERIC, 18, -1},
                {Schema.Type.INT32, INT,
                    JdbcSourceConnectorConfig.NumericMapping.BEST_FIT, NULLABLE, Types.NUMERIC, 8, -1},
                {Schema.Type.INT16, SHORT,
                    JdbcSourceConnectorConfig.NumericMapping.BEST_FIT, NULLABLE, Types.NUMERIC, 3, 0},
                {Schema.Type.INT8, BYTE,
                    JdbcSourceConnectorConfig.NumericMapping.BEST_FIT, NULLABLE, Types.NUMERIC, 1, 0},
                {Schema.Type.BYTES, BIG_DECIMAL,
                    JdbcSourceConnectorConfig.NumericMapping.BEST_FIT, NULLABLE, Types.NUMERIC, 19, -1},

                // floating point - fitting - non-optional
                {Schema.Type.FLOAT64, DOUBLE,
                    JdbcSourceConnectorConfig.NumericMapping.BEST_FIT, NOT_NULLABLE, Types.NUMERIC, 18, 127},
                {Schema.Type.FLOAT64, DOUBLE,
                    JdbcSourceConnectorConfig.NumericMapping.BEST_FIT, NOT_NULLABLE, Types.NUMERIC, 8, 1},
                {Schema.Type.BYTES, BIG_DECIMAL,
                    JdbcSourceConnectorConfig.NumericMapping.BEST_FIT, NOT_NULLABLE, Types.NUMERIC, 19, 1},

                // floating point - fitting - optional
                {Schema.Type.FLOAT64, DOUBLE,
                    JdbcSourceConnectorConfig.NumericMapping.BEST_FIT, NULLABLE, Types.NUMERIC, 18, 127},
                {Schema.Type.FLOAT64, DOUBLE,
                    JdbcSourceConnectorConfig.NumericMapping.BEST_FIT, NULLABLE, Types.NUMERIC, 8, 1},
                {Schema.Type.BYTES, BIG_DECIMAL,
                    JdbcSourceConnectorConfig.NumericMapping.BEST_FIT, NULLABLE, Types.NUMERIC, 19, 1},
            }
        );
    }

    /**
     * Create a {@link JdbcSourceConnectorConfig} with the specified URL and optional config props.
     *
     * @param url           the database URL; may not be null
     * @param propertyPairs optional set of config name-value pairs; must be an even number
     * @return the config; never null
     */
    protected static JdbcSourceConnectorConfig sourceConfigWithUrl(
        final String url,
        final JdbcSourceConnectorConfig.NumericMapping numMapping,
        final String... propertyPairs
    ) {
        final Map<String, String> connProps = new HashMap<>();
        connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
        connProps.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
        connProps.putAll(propertiesFromPairs(propertyPairs));
        connProps.put(JdbcConfig.CONNECTION_URL_CONFIG, url);
        connProps.put(JdbcSourceConnectorConfig.NUMERIC_MAPPING_CONFIG, numMapping.toString());
        return new JdbcSourceConnectorConfig(connProps);
    }

    protected static Map<String, String> propertiesFromPairs(final String... pairs) {
        final Map<String, String> props = new HashMap<>();
        assertThat(pairs.length % 2).as("Expecting even number of properties but found " + pairs.length)
            .isZero();
        for (int i = 0; i != pairs.length; ++i) {
            final String key = pairs[i];
            final String value = pairs[++i];
            props.put(key, value);
        }
        return props;
    }

    @SuppressWarnings("deprecation")
    @ParameterizedTest
    @MethodSource("data")
    public void testValueConversionOnNumeric(final Schema.Type expectedType,
                                             final Object expectedValue,
                                             final JdbcSourceConnectorConfig.NumericMapping numMapping,
                                             final boolean optional,
                                             final int columnType,
                                             final int precision,
                                             final int scale) throws Exception {
        when(columnDefn.precision()).thenReturn(precision);
        when(columnDefn.scale()).thenReturn(scale);
        when(columnDefn.type()).thenReturn(columnType);
        when(columnDefn.isOptional()).thenReturn(optional);
        when(columnDefn.id()).thenReturn(COLUMN_ID);
        when(columnDefn.isSignedNumber()).thenReturn(signed);
        when(columnDefn.typeName()).thenReturn("parameterizedType");

        dialect = new GenericDatabaseDialect(sourceConfigWithUrl("jdbc:some:db", numMapping));
        schemaBuilder = SchemaBuilder.struct();

        // Check the schema field is created with the right type
        dialect.addFieldToSchema(columnDefn, schemaBuilder);
        final Schema schema = schemaBuilder.build();
        final List<Field> fields = schema.fields();
        assertThat(fields).hasSize(1);
        final Field field = fields.get(0);
        assertThat(field.schema().type()).isEqualTo(expectedType);

        // Set up the ResultSet
        when(resultSet.getBigDecimal(1, scale)).thenReturn(BIG_DECIMAL);
        when(resultSet.getBigDecimal(1, -scale)).thenReturn(BIG_DECIMAL);
        when(resultSet.getBigDecimal(1)).thenReturn(BIG_DECIMAL);
        when(resultSet.getLong(1)).thenReturn(LONG);
        when(resultSet.getInt(1)).thenReturn(INT);
        when(resultSet.getShort(1)).thenReturn(SHORT);
        when(resultSet.getByte(1)).thenReturn(BYTE);
        when(resultSet.getDouble(1)).thenReturn(DOUBLE);

        // Check the converter operates correctly
        final ColumnMapping mapping = new ColumnMapping(columnDefn, 1, field);
        converter = dialect.columnConverterFor(
            mapping,
            mapping.columnDefn(),
            mapping.columnNumber(),
            true
        );
        final Object value = converter.convert(resultSet);
        if (value instanceof Number && expectedValue instanceof Number) {
            assertThat(((Number) value).floatValue())
                .isCloseTo(((Number) expectedValue).floatValue(), offset(0.01f));
        } else {
            assertThat(value).isEqualTo(expectedValue);
        }
    }
}
