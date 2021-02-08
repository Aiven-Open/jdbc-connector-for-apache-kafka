/*
 * Copyright 2019 Aiven Oy
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

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.connect.jdbc.config.JdbcConfig;
import io.aiven.connect.jdbc.sink.JdbcSinkConfig;
import io.aiven.connect.jdbc.sink.metadata.SinkRecordField;
import io.aiven.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.aiven.connect.jdbc.util.ColumnId;
import io.aiven.connect.jdbc.util.DateTimeUtils;
import io.aiven.connect.jdbc.util.TableId;

import com.google.common.io.Resources;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(Parameterized.class)
public abstract class BaseDialectTest<T extends GenericDatabaseDialect> {

    protected static final GregorianCalendar EPOCH_PLUS_TEN_THOUSAND_DAYS;
    protected static final GregorianCalendar EPOCH_PLUS_TEN_THOUSAND_MILLIS;
    protected static final GregorianCalendar MARCH_15_2001_MIDNIGHT;

    static {
        EPOCH_PLUS_TEN_THOUSAND_DAYS = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        EPOCH_PLUS_TEN_THOUSAND_DAYS.setTimeZone(TimeZone.getTimeZone("UTC"));
        EPOCH_PLUS_TEN_THOUSAND_DAYS.add(Calendar.DATE, 10000);

        EPOCH_PLUS_TEN_THOUSAND_MILLIS = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        EPOCH_PLUS_TEN_THOUSAND_MILLIS.setTimeZone(TimeZone.getTimeZone("UTC"));
        EPOCH_PLUS_TEN_THOUSAND_MILLIS.add(Calendar.MILLISECOND, 10000);

        MARCH_15_2001_MIDNIGHT = new GregorianCalendar(2001, Calendar.MARCH, 15, 0, 0, 0);
        MARCH_15_2001_MIDNIGHT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Parameterized.Parameters
    public static Collection<Object[]> quoteIdentifiers() {
        return Arrays.asList(new Object[][]{
            {null, true}, {true, true}, {false, false}
        });
    }

    @Parameterized.Parameter(0)
    public /* NOT private */ Boolean quoteIdentifiers;
    @Parameterized.Parameter(1)
    public /* NOT private */ Boolean quoteIdentifiersExpectedBehavior;

    protected TableId tableId;
    protected ColumnId columnPK1;
    protected ColumnId columnPK2;
    protected ColumnId columnA;
    protected ColumnId columnB;
    protected ColumnId columnC;
    protected ColumnId columnD;

    protected List<ColumnId> pkColumns;
    protected List<ColumnId> columnsAtoD;
    protected List<SinkRecordField> sinkRecordFields;
    protected T dialect;
    protected int defaultLoginTimeout;

    @Before
    public void setup() throws Exception {
        defaultLoginTimeout = DriverManager.getLoginTimeout();
        DriverManager.setLoginTimeout(1);

        dialect = createDialect();

        // Set up some data ...
        final Schema optionalDateWithDefault = Date.builder().defaultValue(MARCH_15_2001_MIDNIGHT.getTime())
            .optional().build();
        final Schema optionalTimeWithDefault = Time.builder().defaultValue(MARCH_15_2001_MIDNIGHT.getTime())
            .optional().build();
        final Schema optionalTsWithDefault = Timestamp.builder()
            .defaultValue(MARCH_15_2001_MIDNIGHT.getTime())
            .optional().build();
        final Schema optionalDecimal = Decimal.builder(4).optional().parameter("p1", "v1")
            .parameter("p2", "v2").build();
        tableId = new TableId(null, null, "myTable");
        columnPK1 = new ColumnId(tableId, "id1");
        columnPK2 = new ColumnId(tableId, "id2");
        columnA = new ColumnId(tableId, "columnA");
        columnB = new ColumnId(tableId, "columnB");
        columnC = new ColumnId(tableId, "columnC");
        columnD = new ColumnId(tableId, "columnD");

        pkColumns = Arrays.asList(columnPK1, columnPK2);
        columnsAtoD = Arrays.asList(columnA, columnB, columnC, columnD);

        final SinkRecordField f1 = new SinkRecordField(Schema.INT32_SCHEMA, "c1", true);
        final SinkRecordField f2 = new SinkRecordField(Schema.INT64_SCHEMA, "c2", false);
        final SinkRecordField f3 = new SinkRecordField(Schema.STRING_SCHEMA, "c3", false);
        final SinkRecordField f4 = new SinkRecordField(Schema.OPTIONAL_STRING_SCHEMA, "c4", false);
        final SinkRecordField f5 = new SinkRecordField(optionalDateWithDefault, "c5", false);
        final SinkRecordField f6 = new SinkRecordField(optionalTimeWithDefault, "c6", false);
        final SinkRecordField f7 = new SinkRecordField(optionalTsWithDefault, "c7", false);
        final SinkRecordField f8 = new SinkRecordField(optionalDecimal, "c8", false);
        sinkRecordFields = Arrays.asList(f1, f2, f3, f4, f5, f6, f7, f8);
    }

    @After
    public void teardown() throws Exception {
        DriverManager.setLoginTimeout(defaultLoginTimeout);
    }

    /**
     * Create an instance of the dialect to be tested.
     *
     * @return the dialect; may not be null
     */
    protected abstract T createDialect();

    /**
     * Create a {@link JdbcSourceConnectorConfig} with the specified URL and optional config props.
     *
     * @param url           the database URL; may not be null
     * @param propertyPairs optional set of config name-value pairs; must be an even number
     * @return the config; never null
     */
    protected JdbcSourceConnectorConfig sourceConfigWithUrl(
        final String url,
        final String... propertyPairs
    ) {
        final Map<String, String> connProps = new HashMap<>();
        connProps.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
        connProps.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "test-");
        connProps.putAll(propertiesFromPairs(propertyPairs));
        connProps.put(JdbcConfig.CONNECTION_URL_CONFIG, url);
        if (quoteIdentifiers == null) {
            // no-op
        } else {
            connProps.put(JdbcConfig.SQL_QUOTE_IDENTIFIERS_CONFIG, quoteIdentifiers.toString());
        }
        return new JdbcSourceConnectorConfig(connProps);
    }

    /**
     * Create a {@link JdbcSinkConfig} with the specified URL and optional config props.
     *
     * @param url           the database URL; may not be null
     * @param propertyPairs optional set of config name-value pairs; must be an even number
     * @return the config; never null
     */
    protected JdbcSinkConfig sinkConfigWithUrl(
        final String url,
        final String... propertyPairs
    ) {
        final Map<String, String> connProps = new HashMap<>();
        connProps.putAll(propertiesFromPairs(propertyPairs));
        connProps.put(JdbcConfig.CONNECTION_URL_CONFIG, url);
        return new JdbcSinkConfig(connProps);
    }

    protected void assertDecimalMapping(
        final int scale,
        final String expectedSqlType
    ) {
        assertMapping(expectedSqlType, Decimal.schema(scale));
    }

    protected void assertDateMapping(final String expectedSqlType) {
        assertMapping(expectedSqlType, Date.SCHEMA);
    }

    protected void assertTimeMapping(final String expectedSqlType) {
        assertMapping(expectedSqlType, Time.SCHEMA);
    }

    protected void assertTimestampMapping(final String expectedSqlType) {
        assertMapping(expectedSqlType, Timestamp.SCHEMA);
    }

    protected void assertPrimitiveMapping(
        final Schema.Type type,
        final String expectedSqlType
    ) {
        assertMapping(expectedSqlType, type, null);
    }

    protected void assertMapping(
        final String expectedSqlType,
        final Schema schema
    ) {
        assertMapping(expectedSqlType, schema.type(), schema.name(), schema.parameters());
    }

    protected void assertMapping(
        final String expectedSqlType,
        final Schema.Type type,
        final String schemaName,
        final Map<String, String> schemaParams
    ) {
        final SchemaBuilder schemaBuilder = new SchemaBuilder(type).name(schemaName);
        if (schemaParams != null) {
            for (final Map.Entry<String, String> entry : schemaParams.entrySet()) {
                schemaBuilder.parameter(entry.getKey(), entry.getValue());
            }
        }
        final SinkRecordField field = new SinkRecordField(schemaBuilder.build(), schemaName, false);
        final String sqlType = dialect.getSqlType(field);
        assertEquals(expectedSqlType, sqlType);
    }

    protected void assertMapping(
        final String expectedSqlType,
        final Schema.Type type,
        final String schemaName,
        final String... schemaParamPairs
    ) {
        final Map<String, String> schemaProps = propertiesFromPairs(schemaParamPairs);
        assertMapping(expectedSqlType, type, schemaName, schemaProps);
    }


    protected Map<String, String> propertiesFromPairs(final String... pairs) {
        final Map<String, String> props = new HashMap<>();
        assertEquals("Expecting even number of properties but found " + pairs.length, 0,
            pairs.length % 2);
        for (int i = 0; i != pairs.length; ++i) {
            final String key = pairs[i];
            final String value = pairs[++i];
            props.put(key, value);
        }
        return props;
    }

    protected void assertStatements(final String[] expected, final List<String> actual) {
        // TODO: Remove
        assertEquals(expected.length, actual.size());
        for (int i = 0; i != expected.length; ++i) {
            assertQueryEquals(expected[i], actual.get(i));
        }
    }

    protected TableId tableId(final String name) {
        return new TableId(null, null, name);
    }

    protected Collection<ColumnId> columns(final TableId id, final String... names) {
        final List<ColumnId> columns = new ArrayList<>();
        for (int i = 0; i != names.length; ++i) {
            columns.add(new ColumnId(id, names[i]));
        }
        return columns;
    }

    protected void verifyDataTypeMapping(final String expected, final Schema schema) {
        final SinkRecordField field = new SinkRecordField(schema, schema.name(), schema.isOptional());
        assertEquals(expected, dialect.getSqlType(field));
    }

    protected void verifyCreateOneColNoPk(final String expected) {
        assertQueryEquals(expected, dialect.buildCreateTableStatement(tableId, Arrays.asList(
            new SinkRecordField(Schema.INT32_SCHEMA, "col1", false)
        )));
    }

    protected void verifyCreateOneColOnePk(final String expected) {
        assertQueryEquals(expected, dialect.buildCreateTableStatement(tableId, Arrays.asList(
            new SinkRecordField(Schema.INT32_SCHEMA, "pk1", true)
        )));
    }

    protected void verifyCreateThreeColTwoPk(final String expected) {
        assertQueryEquals(expected, dialect.buildCreateTableStatement(tableId, Arrays.asList(
            new SinkRecordField(Schema.INT32_SCHEMA, "pk1", true),
            new SinkRecordField(Schema.INT32_SCHEMA, "pk2", true),
            new SinkRecordField(Schema.INT32_SCHEMA, "col1", false)
        )));
    }

    protected void verifyAlterAddOneCol(final String... expected) {
        assertArrayEquals(expected, dialect.buildAlterTable(tableId, Arrays.asList(
            new SinkRecordField(Schema.OPTIONAL_INT32_SCHEMA, "newcol1", false)
        )).toArray());
    }

    protected void verifyAlterAddTwoCols(final String... expected) {
        assertArrayEquals(expected, dialect.buildAlterTable(tableId, Arrays.asList(
            new SinkRecordField(Schema.OPTIONAL_INT32_SCHEMA, "newcol1", false),
            new SinkRecordField(SchemaBuilder.int32().defaultValue(42).build(), "newcol2", false)
        )).toArray());
    }

    @Test
    public void bindFieldPrimitiveValues() throws SQLException {
        int index = ThreadLocalRandom.current().nextInt();
        verifyBindField(++index, Schema.INT8_SCHEMA, (byte) 42).setByte(index, (byte) 42);
        verifyBindField(++index, Schema.INT16_SCHEMA, (short) 42).setShort(index, (short) 42);
        verifyBindField(++index, Schema.INT32_SCHEMA, 42).setInt(index, 42);
        verifyBindField(++index, Schema.INT64_SCHEMA, 42L).setLong(index, 42L);
        verifyBindField(++index, Schema.BOOLEAN_SCHEMA, false).setBoolean(index, false);
        verifyBindField(++index, Schema.BOOLEAN_SCHEMA, true).setBoolean(index, true);
        verifyBindField(++index, Schema.FLOAT32_SCHEMA, -42f).setFloat(index, -42f);
        verifyBindField(++index, Schema.FLOAT64_SCHEMA, 42d).setDouble(index, 42d);
        verifyBindField(++index, Schema.BYTES_SCHEMA, new byte[]{42}).setBytes(index, new byte[]{42});
        verifyBindField(++index, Schema.BYTES_SCHEMA,
            ByteBuffer.wrap(new byte[]{42})).setBytes(index, new byte[]{42});
        verifyBindField(++index, Schema.STRING_SCHEMA, "yep").setString(index, "yep");
        verifyBindField(
            ++index,
            Decimal.schema(0),
            new BigDecimal("1.5").setScale(0, BigDecimal.ROUND_HALF_EVEN)
        ).setBigDecimal(index, new BigDecimal(2));
        final Calendar utcCalendar = DateTimeUtils.getTimeZoneCalendar(TimeZone.getTimeZone(ZoneOffset.UTC));
        verifyBindField(
            ++index,
            Date.SCHEMA,
            new java.util.Date(0)
        ).setDate(index, new java.sql.Date(0), utcCalendar);
        verifyBindField(
            ++index,
            Time.SCHEMA,
            new java.util.Date(1000)
        ).setTime(index, new java.sql.Time(1000), utcCalendar);
        verifyBindField(
            ++index,
            Timestamp.SCHEMA,
            new java.util.Date(100)
        ).setTimestamp(index, new java.sql.Timestamp(100), utcCalendar);
    }

    @Test
    public void bindFieldNull() throws SQLException {
        final List<Schema> nullableTypes = Arrays.asList(
            Schema.INT8_SCHEMA,
            Schema.INT16_SCHEMA,
            Schema.INT32_SCHEMA,
            Schema.INT64_SCHEMA,
            Schema.FLOAT32_SCHEMA,
            Schema.FLOAT64_SCHEMA,
            Schema.BOOLEAN_SCHEMA,
            Schema.BYTES_SCHEMA,
            Schema.STRING_SCHEMA,
            Decimal.schema(0),
            Date.SCHEMA,
            Time.SCHEMA,
            Timestamp.SCHEMA
        );
        int index = 0;
        for (final Schema schema : nullableTypes) {
            verifyBindField(++index, schema, null).setObject(index, null);
        }
    }

    @Test(expected = ConnectException.class)
    public void bindFieldStructUnsupported() throws SQLException {
        final Schema structSchema = SchemaBuilder.struct().field("test", Schema.BOOLEAN_SCHEMA).build();
        dialect.bindField(mock(PreparedStatement.class), 1, structSchema, new Struct(structSchema));
    }

    @Test(expected = ConnectException.class)
    public void bindFieldArrayUnsupported() throws SQLException {
        final Schema arraySchema = SchemaBuilder.array(Schema.INT8_SCHEMA);
        dialect.bindField(mock(PreparedStatement.class), 1, arraySchema, Collections.emptyList());
    }

    @Test(expected = ConnectException.class)
    public void bindFieldMapUnsupported() throws SQLException {
        final Schema mapSchema = SchemaBuilder.map(Schema.INT8_SCHEMA, Schema.INT8_SCHEMA);
        dialect.bindField(mock(PreparedStatement.class), 1, mapSchema, Collections.emptyMap());
    }

    protected void assertSanitizedUrl(final String url, final String expectedSanitizedUrl) {
        assertEquals(expectedSanitizedUrl, dialect.sanitizedUrl(url));
    }

    protected void assertQueryEquals(final String expected, final String actual) {
        assertEquals(expected, actual);
    }

    protected PreparedStatement verifyBindField(final int index, final Schema schema, final Object value)
        throws SQLException {
        final PreparedStatement statement = mock(PreparedStatement.class);
        dialect.bindField(statement, index, schema, value);
        return verify(statement, times(1));
    }

    /**
     * Read the content of a query located in a resource file specific
     * for this test class and {@link BaseDialectTest#quoteIdentifiersExpectedBehavior},
     * i.e. located at {@code {class_short_name}/{queryName}-[non]delimited.txt}.
     *
     * <p>Unix line feed <code>\n</code> will be replaced with the
     * current system's.
     *
     * <p>{@code |<EOL>} will be removed from the output.
     *
     * @param queryName the resource file name without path.
     * @return content of the resource file.
     */
    final String readQueryResourceForThisTest(final String queryName) {
        final String filename = queryName + getQueryResourceDelimitedSuffix();
        try {
            final String resourceName = Paths.get(
                "io.aiven.connect.jdbc.dialect", getClass().getSimpleName(), filename).toString();
            final URL url = Resources.getResource(resourceName);
            final String content = Resources.toString(url, StandardCharsets.UTF_8);
            return content
                .replace("\n", System.lineSeparator())
                .replace("|<EOL>", "");
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Read the content of a query located in a resource file specific
     * for this test class and {@link BaseDialectTest#quoteIdentifiersExpectedBehavior},
     * i.e. located at {@code {class_short_name}/{queryName}-[non]delimited.txt}.
     *
     * <p>The content is read as an array of strings.
     *
     * @param queryName the resource file name without path.
     * @return content of the resource file (array of strings).
     */
    final String[] readQueryResourceLinesForThisTest(final String queryName) {
        final String filename = queryName + getQueryResourceDelimitedSuffix();
        try {
            final String resourceName = Paths.get(
                "io.aiven.connect.jdbc.dialect", getClass().getSimpleName(), filename).toString();
            final URL url = Resources.getResource(resourceName);
            final String content = Resources.toString(url, StandardCharsets.UTF_8);
            return content.split("\n");
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getQueryResourceDelimitedSuffix() {
        return quoteIdentifiersExpectedBehavior ? "-quoted.txt" : "-nonquoted.txt";
    }
}
