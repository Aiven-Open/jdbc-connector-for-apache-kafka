/*
 * Copyright 2019 Aiven Oy
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

package io.aiven.connect.jdbc.sink;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

import io.aiven.connect.jdbc.config.JdbcConfig;
import io.aiven.connect.jdbc.util.DateTimeUtils;

import org.easymock.EasyMockSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JdbcSinkTaskTest extends EasyMockSupport {
    private final SqliteHelper sqliteHelper = new SqliteHelper(getClass().getSimpleName());

    private static final Schema SCHEMA = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("age", Schema.OPTIONAL_INT32_SCHEMA)
        .field("bool", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("short", Schema.OPTIONAL_INT16_SCHEMA)
        .field("byte", Schema.OPTIONAL_INT8_SCHEMA)
        .field("long", Schema.OPTIONAL_INT64_SCHEMA)
        .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA)
        .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .field("modified", Timestamp.SCHEMA)
        .build();

    @Before
    public void setUp() throws IOException, SQLException {
        sqliteHelper.setUp();
    }

    @After
    public void tearDown() throws IOException, SQLException {
        sqliteHelper.tearDown();
    }

    @Test
    public void putPropagatesToDbWithAutoCreateAndPkModeKafka() throws Exception {
        final Map<String, String> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("auto.create", "true");
        props.put("pk.mode", "kafka");
        props.put("pk.fields", "kafka_topic,kafka_partition,kafka_offset");
        final String timeZoneID = "America/Los_Angeles";
        final TimeZone timeZone = TimeZone.getTimeZone(timeZoneID);
        props.put("db.timezone", timeZoneID);

        final JdbcSinkTask task = new JdbcSinkTask();
        task.initialize(mock(SinkTaskContext.class));

        task.start(props);

        final Struct struct = new Struct(SCHEMA)
            .put("firstName", "Alex")
            .put("lastName", "Smith")
            .put("bool", true)
            .put("short", (short) 1234)
            .put("byte", (byte) -32)
            .put("long", 12425436L)
            .put("float", (float) 2356.3)
            .put("double", -2436546.56457)
            .put("age", 21)
            .put("modified", new Date(1474661402123L));

        final String topic = "atopic";

        task.put(Collections.singleton(
            new SinkRecord(topic, 1, null, null, SCHEMA, struct, 42)
        ));

        assertEquals(
            1,
            sqliteHelper.select(
                "SELECT * FROM " + topic,
                new SqliteHelper.ResultSetReadCallback() {
                    @Override
                    public void read(final ResultSet rs) throws SQLException {
                        assertEquals(topic, rs.getString("kafka_topic"));
                        assertEquals(1, rs.getInt("kafka_partition"));
                        assertEquals(42, rs.getLong("kafka_offset"));
                        assertEquals(struct.getString("firstName"), rs.getString("firstName"));
                        assertEquals(struct.getString("lastName"), rs.getString("lastName"));
                        assertEquals(struct.getBoolean("bool"), rs.getBoolean("bool"));
                        assertEquals(struct.getInt8("byte").byteValue(), rs.getByte("byte"));
                        assertEquals(struct.getInt16("short").shortValue(), rs.getShort("short"));
                        assertEquals(struct.getInt32("age").intValue(), rs.getInt("age"));
                        assertEquals(struct.getInt64("long").longValue(), rs.getLong("long"));
                        assertEquals(struct.getFloat32("float"), rs.getFloat("float"), 0.01);
                        assertEquals(struct.getFloat64("double"), rs.getDouble("double"), 0.01);
                        final java.sql.Timestamp dbTimestamp = rs.getTimestamp(
                            "modified",
                            DateTimeUtils.getTimeZoneCalendar(timeZone)
                        );
                        assertEquals(((java.util.Date) struct.get("modified")).getTime(), dbTimestamp.getTime());
                    }
                }
            )
        );
    }

    @Test
    public void putPropagatesToDbWithPkModeRecordValue() throws Exception {
        final Map<String, String> props = new HashMap<>();
        props.put("connection.url", sqliteHelper.sqliteUri());
        props.put("pk.mode", "record_value");
        props.put("pk.fields", "firstName,lastName");

        final JdbcSinkTask task = new JdbcSinkTask();
        task.initialize(mock(SinkTaskContext.class));

        final String topic = "atopic";

        sqliteHelper.createTable(
            "CREATE TABLE " + topic + "("
                + "    firstName  TEXT,"
                + "    lastName  TEXT,"
                + "    age INTEGER,"
                + "    bool  NUMERIC,"
                + "    byte  INTEGER,"
                + "    short INTEGER NULL,"
                + "    long INTEGER,"
                + "    float NUMERIC,"
                + "    double NUMERIC,"
                + "    bytes BLOB,"
                + "    modified DATETIME, "
                + "PRIMARY KEY (firstName, lastName));"
        );

        task.start(props);

        final Struct struct = new Struct(SCHEMA)
            .put("firstName", "Christina")
            .put("lastName", "Brams")
            .put("bool", false)
            .put("byte", (byte) -72)
            .put("long", 8594L)
            .put("double", 3256677.56457d)
            .put("age", 28)
            .put("modified", new Date(1474661402123L));

        task.put(Collections.singleton(new SinkRecord(topic, 1, null, null, SCHEMA, struct, 43)));

        final String query = "SELECT * FROM "
            + topic
            + " WHERE firstName='"
            + struct.getString("firstName")
            + "' and lastName='"
            + struct.getString("lastName")
            + "'";
        assertEquals(
            1,
            sqliteHelper.select(
                query,
                new SqliteHelper.ResultSetReadCallback() {
                    @Override
                    public void read(final ResultSet rs) throws SQLException {
                        assertEquals(struct.getBoolean("bool"), rs.getBoolean("bool"));
                        rs.getShort("short");
                        assertTrue(rs.wasNull());
                        assertEquals(struct.getInt8("byte").byteValue(), rs.getByte("byte"));
                        assertEquals(struct.getInt32("age").intValue(), rs.getInt("age"));
                        assertEquals(struct.getInt64("long").longValue(), rs.getLong("long"));
                        rs.getShort("float");
                        assertTrue(rs.wasNull());
                        assertEquals(struct.getFloat64("double"), rs.getDouble("double"), 0.01);
                        final java.sql.Timestamp dbTimestamp = rs.getTimestamp(
                            "modified",
                            DateTimeUtils.getTimeZoneCalendar(TimeZone.getTimeZone(ZoneOffset.UTC))
                        );
                        assertEquals(((java.util.Date) struct.get("modified")).getTime(), dbTimestamp.getTime());
                    }
                }
            )
        );
    }

    @Test
    public void retries() throws SQLException {
        final int maxRetries = 2;
        final int retryBackoffMs = 1000;

        final Set<SinkRecord> records = Collections.singleton(new SinkRecord("stub", 0, null, null, null, null, 0));
        final JdbcDbWriter mockWriter = createMock(JdbcDbWriter.class);
        final SinkTaskContext ctx = createMock(SinkTaskContext.class);

        mockWriter.write(records);
        expectLastCall().andThrow(new SQLException()).times(1 + maxRetries);

        ctx.timeout(retryBackoffMs);
        expectLastCall().times(maxRetries);

        mockWriter.closeQuietly();
        expectLastCall().times(maxRetries);

        final JdbcSinkTask task = new JdbcSinkTask() {
            @Override
            void initWriter() {
                this.writer = mockWriter;
            }
        };
        task.initialize(ctx);

        final Map<String, String> props = new HashMap<>();
        props.put(JdbcConfig.CONNECTION_URL_CONFIG, "stub");
        props.put(JdbcSinkConfig.MAX_RETRIES, String.valueOf(maxRetries));
        props.put(JdbcSinkConfig.RETRY_BACKOFF_MS, String.valueOf(retryBackoffMs));
        task.start(props);

        replayAll();

        try {
            task.put(records);
            fail();
        } catch (final RetriableException expected) {
        }

        try {
            task.put(records);
            fail();
        } catch (final RetriableException expected) {
        }

        try {
            task.put(records);
            fail();
        } catch (final RetriableException e) {
            fail("Non-retriable exception expected");
        } catch (final ConnectException expected) {
            assertEquals(SQLException.class, expected.getCause().getClass());
        }

        verifyAll();
    }

}
