/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
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
 */

package io.aiven.connect.jdbc.source;

import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

import io.aiven.connect.jdbc.config.JdbcConfig;
import io.aiven.connect.jdbc.util.DateTimeUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.verify;

// Tests of polling that return data updates, i.e. verifies the different behaviors for getting
// incremental data updates from the database
@ExtendWith(MockitoExtension.class)
public class JdbcSourceTaskUpdateTest extends JdbcSourceTaskTestBase {
    private static final Map<String, String> QUERY_SOURCE_PARTITION
        = Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
        JdbcSourceConnectorConstants.QUERY_NAME_VALUE);

    private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone(ZoneOffset.UTC);

    @AfterEach
    public void tearDown() throws Exception {
        task.stop();
        super.tearDown();
    }

    @Test
    public void testBulkPeriodicLoad() throws Exception {
        final EmbeddedDerby.ColumnName column = new EmbeddedDerby.ColumnName("id");
        db.createTable(SINGLE_TABLE_NAME, "id", "INT NOT NULL");
        db.insert(SINGLE_TABLE_NAME, "id", 1);

        // Bulk periodic load is currently the default
        task.start(singleTableConfig());

        List<SourceRecord> records = task.poll();
        assertThat(countIntValues(records, "id")).containsExactly(entry(1, 1));
        assertRecordsTopic(records, TOPIC_PREFIX + SINGLE_TABLE_NAME);

        records = pollRecords(task);
        assertThat(countIntValues(records, "id")).containsExactly(entry(1, 1));
        assertRecordsTopic(records, TOPIC_PREFIX + SINGLE_TABLE_NAME);

        db.insert(SINGLE_TABLE_NAME, "id", 2);
        records = pollRecords(task);
        final Map<Integer, Integer> twoRecords = new HashMap<>();
        twoRecords.put(1, 1);
        twoRecords.put(2, 1);
        assertThat(countIntValues(records, "id")).isEqualTo(twoRecords);
        assertRecordsTopic(records, TOPIC_PREFIX + SINGLE_TABLE_NAME);

        db.delete(SINGLE_TABLE_NAME, new EmbeddedDerby.EqualsCondition(column, 1));
        records = pollRecords(task);
        assertThat(countIntValues(records, "id")).containsExactly(entry(2, 1));
        assertRecordsTopic(records, TOPIC_PREFIX + SINGLE_TABLE_NAME);
    }

    @Test
    public void testIncrementingInvalidColumn() throws Exception {
        expectInitializeNoOffsets(Arrays.asList(
            SINGLE_TABLE_PARTITION_WITH_VERSION,
            SINGLE_TABLE_PARTITION)
        );

        // Incrementing column must be NOT NULL
        db.createTable(SINGLE_TABLE_NAME, "id", "INT");

        assertThatThrownBy(() -> startTask(null, "id", null))
            .isInstanceOf(ConnectException.class);
    }

    @Test
    public void testTimestampInvalidColumn() throws Exception {
        expectInitializeNoOffsets(Arrays.asList(
            SINGLE_TABLE_PARTITION_WITH_VERSION,
            SINGLE_TABLE_PARTITION)
        );

        // Timestamp column must be NOT NULL
        db.createTable(SINGLE_TABLE_NAME, "modified", "TIMESTAMP");

        assertThatThrownBy(() -> startTask("modified", null, null))
            .isInstanceOf(ConnectException.class);
    }

    @Test
    public void testManualIncrementing() throws Exception {
        manualIncrementingInternal(null, List.of(0));
    }

    @Test
    public void testManualIncrementingManualId() throws Exception {
        manualIncrementingInternal(-1L, List.of(0));
    }

    @Test
    public void testManualIncrementingManualCustomId() throws Exception {
        manualIncrementingInternal(-2L, Arrays.asList(-1, 0));
    }

    private void manualIncrementingInternal(final Long initialId, final List<Integer> expectedIds) throws Exception {
        expectInitializeNoOffsets(Arrays.asList(
            SINGLE_TABLE_PARTITION_WITH_VERSION,
            SINGLE_TABLE_PARTITION)
        );


        db.createTable(SINGLE_TABLE_NAME,
            "id", "INT NOT NULL");
        // Records with id > initialId are picked
        db.insert(SINGLE_TABLE_NAME, "id", -2);
        db.insert(SINGLE_TABLE_NAME, "id", -1);
        db.insert(SINGLE_TABLE_NAME, "id", 0);

        startTask(null, "id", null, 0L, "UTC", initialId, null);
        verifyPoll(expectedIds.size(), "id", expectedIds, false, true, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

        // Adding records should result in only those records during the next poll()
        db.insert(SINGLE_TABLE_NAME, "id", 2);
        db.insert(SINGLE_TABLE_NAME, "id", 3);

        verifyPoll(2, "id", Arrays.asList(2, 3), false, true, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

        verify(taskContext).offsetStorageReader();
    }

    @Test
    public void testAutoincrement() throws Exception {
        expectInitializeNoOffsets(Arrays.asList(
            SINGLE_TABLE_PARTITION_WITH_VERSION,
            SINGLE_TABLE_PARTITION)
        );

        final String extraColumn = "col";
        // Need extra column to be able to insert anything, extra is ignored.
        db.createTable(SINGLE_TABLE_NAME,
            "id", "INT NOT NULL GENERATED ALWAYS AS IDENTITY",
            extraColumn, "FLOAT");
        db.insert(SINGLE_TABLE_NAME, extraColumn, 32.4f);

        startTask(null, "", null); // auto-incrementing
        verifyIncrementingFirstPoll(TOPIC_PREFIX + SINGLE_TABLE_NAME);

        // Adding records should result in only those records during the next poll()
        db.insert(SINGLE_TABLE_NAME, extraColumn, 33.4f);
        db.insert(SINGLE_TABLE_NAME, extraColumn, 35.4f);

        verifyPoll(2, "id", Arrays.asList(2, 3), false, true, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

        verify(taskContext).offsetStorageReader();
    }

    @Test
    public void testTimestamp() throws Exception {
        timestampInternal(null, List.of(1));
    }

    @Test
    public void testTimestampManualOffset() throws Exception {
        timestampInternal(0L, List.of(1));
    }

    @Test
    public void testTimestampCustomOffset() throws Exception {
        timestampInternal(-15L, Arrays.asList(-1, 0, 1));
    }

    private void timestampInternal(final Long timestampInitialMs, final List<Integer> expectedIds) throws Exception {
        expectInitializeNoOffsets(Arrays.asList(
            SINGLE_TABLE_PARTITION_WITH_VERSION,
            SINGLE_TABLE_PARTITION)
        );

        // Manage these manually so we can verify the emitted values
        db.createTable(SINGLE_TABLE_NAME,
            "modified", "TIMESTAMP NOT NULL",
            "id", "INT");
        db.insert(SINGLE_TABLE_NAME,
                "modified", DateTimeUtils.formatTimestamp(new Timestamp(-20L), UTC_TIME_ZONE),
                "id", -2);
        db.insert(SINGLE_TABLE_NAME,
                "modified", DateTimeUtils.formatTimestamp(new Timestamp(-10L), UTC_TIME_ZONE),
                "id", -1);
        db.insert(SINGLE_TABLE_NAME,
                "modified", DateTimeUtils.formatTimestamp(new Timestamp(0L), UTC_TIME_ZONE),
                "id", 0);
        db.insert(SINGLE_TABLE_NAME,
                "modified", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
                "id", 1);

        startTask("modified", null, null, 0L, "UTC", null, timestampInitialMs);
        verifyPoll(expectedIds.size(), "id", expectedIds, true, false, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

        // If there isn't enough resolution, this could miss some rows. In this case, we'll only see
        // IDs 3 & 4.
        db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
            "id", 2);
        db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(11L), UTC_TIME_ZONE),
            "id", 3);
        db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(12L), UTC_TIME_ZONE),
            "id", 4);

        verifyPoll(2, "id", Arrays.asList(3, 4), true, false, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

        verify(taskContext).offsetStorageReader();
    }

    @Test
    public void testMultiColumnTimestamp() throws Exception {
        expectInitializeNoOffsets(Arrays.asList(
            SINGLE_TABLE_PARTITION_WITH_VERSION,
            SINGLE_TABLE_PARTITION)
        );

        // Manage these manually so we can verify the emitted values
        db.createTable(SINGLE_TABLE_NAME,
            "modified", "TIMESTAMP",
            "created", "TIMESTAMP NOT NULL",
            "id", "INT");
        db.insert(SINGLE_TABLE_NAME,
            "created", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
            "id", 1);
        startTask("modified, created", null, null);
        verifyMultiTimestampFirstPoll(TOPIC_PREFIX + SINGLE_TABLE_NAME);

        db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(13L), UTC_TIME_ZONE),
            "created", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
            "id", 2);
        db.insert(SINGLE_TABLE_NAME,
            "created", DateTimeUtils.formatTimestamp(new Timestamp(11L), UTC_TIME_ZONE),
            "id", 3);
        db.insert(SINGLE_TABLE_NAME,
            "created", DateTimeUtils.formatTimestamp(new Timestamp(12L), UTC_TIME_ZONE),
            "id", 4);

        verifyPoll(3, "id", Arrays.asList(2, 3, 4), false, false, true, TOPIC_PREFIX + SINGLE_TABLE_NAME);

        verify(taskContext).offsetStorageReader();
    }

    @Test
    public void testTimestampWithDelay() throws Exception {
        expectInitializeNoOffsets(Arrays.asList(
            SINGLE_TABLE_PARTITION_WITH_VERSION,
            SINGLE_TABLE_PARTITION)
        );

        // Manage these manually so we can verify the emitted values
        db.createTable(SINGLE_TABLE_NAME,
            "modified", "TIMESTAMP NOT NULL",
            "id", "INT");

        db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
            "id", 1);

        startTask("modified", null, null, 4L, "UTC", null, null);
        verifyTimestampFirstPoll(TOPIC_PREFIX + SINGLE_TABLE_NAME);

        final Long currentTime = new Date().getTime();

        // Validate that we are seeing 2,3 but not 4,5 as they are getting delayed to the next round
        // using "toString" and not UTC because Derby's current_timestamp is always local time
        // (i.e. doesn't honor Calendar settings)
        db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(currentTime).toString(), "id", 2);
        db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(currentTime + 1L).toString(), "id", 3);
        db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(currentTime + 500L).toString(), "id", 4);
        db.insert(SINGLE_TABLE_NAME, "modified", new Timestamp(currentTime + 501L).toString(), "id", 5);

        // avoid flaky test where only 1 record gets received
        Thread.sleep(1);
        verifyPoll(2, "id", Arrays.asList(2, 3), true, false, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

        // make sure we get the rest
        Thread.sleep(500);
        verifyPoll(2, "id", Arrays.asList(4, 5), true, false, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

        verify(taskContext).offsetStorageReader();
    }


    @Test
    public void testTimestampAndIncrementing() throws Exception {
        expectInitializeNoOffsets(Arrays.asList(
            SINGLE_TABLE_PARTITION_WITH_VERSION,
            SINGLE_TABLE_PARTITION)
        );

        // Manage these manually so we can verify the emitted values
        db.createTable(SINGLE_TABLE_NAME,
            "modified", "TIMESTAMP NOT NULL",
            "id", "INT NOT NULL");
        db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
            "id", 1);

        startTask("modified", "id", null);
        verifyIncrementingAndTimestampFirstPoll(TOPIC_PREFIX + SINGLE_TABLE_NAME);

        // Should be able to pick up id 3 because of ID despite same timestamp.
        // Note this is setup so we can reuse some validation code
        db.insert(SINGLE_TABLE_NAME, "modified",
            DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE), "id", 3);
        db.insert(SINGLE_TABLE_NAME, "modified",
            DateTimeUtils.formatTimestamp(new Timestamp(11L), UTC_TIME_ZONE), "id", 1);

        verifyPoll(2, "id", Arrays.asList(3, 1), true, true, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

        verify(taskContext).offsetStorageReader();
    }

    @Test
    public void testTimestampInNonUtcTimezone() throws Exception {
        expectInitializeNoOffsets(Arrays.asList(
            SINGLE_TABLE_PARTITION_WITH_VERSION,
            SINGLE_TABLE_PARTITION)
        );

        final String timeZoneID = "America/Los_Angeles";
        final TimeZone timeZone = TimeZone.getTimeZone(timeZoneID);
        // Manage these manually so we can verify the emitted values
        db.createTable(SINGLE_TABLE_NAME,
            "modified", "TIMESTAMP NOT NULL",
            "id", "INT NOT NULL");
        final String modifiedTimestamp = DateTimeUtils.formatTimestamp(new Timestamp(10L), timeZone);
        db.insert(SINGLE_TABLE_NAME, "modified", modifiedTimestamp, "id", 1);

        startTask("modified", "id", null, 0L, timeZoneID, null, null);
        verifyIncrementingAndTimestampFirstPoll(TOPIC_PREFIX + SINGLE_TABLE_NAME);

    }

    @Test
    public void testTimestampInInvalidTimezone() throws Exception {
        final String invalidTimeZoneID = "Europe/Invalid";
        // Manage these manually so we can verify the emitted values
        db.createTable(SINGLE_TABLE_NAME,
            "modified", "TIMESTAMP NOT NULL",
            "id", "INT NOT NULL");

        assertThatThrownBy(() ->
            startTask("modified", "id", null, 0L, invalidTimeZoneID, null, null))
            .isInstanceOf(ConnectException.class)
            .hasCauseInstanceOf(ConfigException.class)
            .hasRootCauseMessage(
                "Invalid value Europe/Invalid for configuration db.timezone: Invalid time zone identifier");
    }

    @Test
    public void testMultiColumnTimestampAndIncrementing() throws Exception {
        expectInitializeNoOffsets(Arrays.asList(
            SINGLE_TABLE_PARTITION_WITH_VERSION,
            SINGLE_TABLE_PARTITION)
        );


        // Manage these manually so we can verify the emitted values
        db.createTable(SINGLE_TABLE_NAME,
            "modified", "TIMESTAMP",
            "created", "TIMESTAMP NOT NULL",
            "id", "INT NOT NULL");
        db.insert(SINGLE_TABLE_NAME,
            "created", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
            "id", 1);

        startTask("modified, created", "id", null);
        verifyIncrementingAndMultiTimestampFirstPoll(TOPIC_PREFIX + SINGLE_TABLE_NAME);

        db.insert(SINGLE_TABLE_NAME, "created",
            DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE), "id", 3);
        db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(11L), UTC_TIME_ZONE),
            "created", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
            "id", 1);

        verifyPoll(2, "id", Arrays.asList(3, 1), false, true, true, TOPIC_PREFIX + SINGLE_TABLE_NAME);

        verify(taskContext).offsetStorageReader();
    }

    @Test
    public void testManualIncrementingRestoreNoVersionOffset() throws Exception {
        final TimestampIncrementingOffset offset = new TimestampIncrementingOffset(null, 1L);
        testManualIncrementingRestoreOffset(
            Collections.singletonMap(SINGLE_TABLE_PARTITION, offset.toMap())
        );
    }

    @Test
    public void testManualIncrementingRestoreVersionOneOffset() throws Exception {
        final TimestampIncrementingOffset offset = new TimestampIncrementingOffset(null, 1L);
        testManualIncrementingRestoreOffset(
            Collections.singletonMap(SINGLE_TABLE_PARTITION_WITH_VERSION, offset.toMap())
        );
    }

    @Test
    public void testManualIncrementingRestoreOffsetsWithMultipleProtocol() throws Exception {
        final TimestampIncrementingOffset oldOffset = new TimestampIncrementingOffset(null, 0L);
        final TimestampIncrementingOffset offset = new TimestampIncrementingOffset(null, 1L);
        final Map<Map<String, String>, Map<String, Object>> offsets = new HashMap<>();
        offsets.put(SINGLE_TABLE_PARTITION_WITH_VERSION, offset.toMap());
        offsets.put(SINGLE_TABLE_PARTITION, oldOffset.toMap());
        //we want to always use the offset with the latest protocol found
        testManualIncrementingRestoreOffset(offsets);
    }

    private void testManualIncrementingRestoreOffset(
        final Map<Map<String, String>, Map<String, Object>> offsets) throws Exception {
        expectInitialize(
            Arrays.asList(SINGLE_TABLE_PARTITION_WITH_VERSION, SINGLE_TABLE_PARTITION),
            offsets
        );

        db.createTable(SINGLE_TABLE_NAME, "id", "INT NOT NULL");
        db.insert(SINGLE_TABLE_NAME, "id", 1);
        db.insert(SINGLE_TABLE_NAME, "id", 2);
        db.insert(SINGLE_TABLE_NAME, "id", 3);

        startTask(null, "id", null);

        // Effectively skips first poll
        verifyPoll(2, "id", Arrays.asList(2, 3), false, true, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

        verify(taskContext).offsetStorageReader();
    }

    @Test
    public void testAutoincrementRestoreNoVersionOffset() throws Exception {
        final TimestampIncrementingOffset offset = new TimestampIncrementingOffset(null, 1L);
        testAutoincrementRestoreOffset(
            Collections.singletonMap(SINGLE_TABLE_PARTITION, offset.toMap())
        );
    }

    @Test
    public void testAutoincrementRestoreVersionOneOffset() throws Exception {
        final TimestampIncrementingOffset offset = new TimestampIncrementingOffset(null, 1L);
        testAutoincrementRestoreOffset(
            Collections.singletonMap(SINGLE_TABLE_PARTITION_WITH_VERSION, offset.toMap())
        );
    }

    @Test
    public void testAutoincrementRestoreOffsetsWithMultipleProtocol() throws Exception {
        final TimestampIncrementingOffset oldOffset = new TimestampIncrementingOffset(null, 0L);
        final TimestampIncrementingOffset offset = new TimestampIncrementingOffset(null, 1L);
        final Map<Map<String, String>, Map<String, Object>> offsets = new HashMap<>();
        offsets.put(SINGLE_TABLE_PARTITION_WITH_VERSION, offset.toMap());
        offsets.put(SINGLE_TABLE_PARTITION, oldOffset.toMap());
        //we want to always use the offset with the latest protocol found
        testAutoincrementRestoreOffset(offsets);
    }

    private void testAutoincrementRestoreOffset(
        final Map<Map<String, String>, Map<String, Object>> offsets) throws Exception {

        expectInitialize(Arrays.asList(
            SINGLE_TABLE_PARTITION_WITH_VERSION, SINGLE_TABLE_PARTITION),
            offsets
        );

        final String extraColumn = "col";
        // Use BIGINT here to test LONG columns
        db.createTable(SINGLE_TABLE_NAME,
            "id", "BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY",
            extraColumn, "FLOAT");
        db.insert(SINGLE_TABLE_NAME, extraColumn, 32.4f);
        db.insert(SINGLE_TABLE_NAME, extraColumn, 33.4f);
        db.insert(SINGLE_TABLE_NAME, extraColumn, 35.4f);

        startTask(null, "", null); // autoincrementing

        // Effectively skips first poll
        verifyPoll(2, "id", Arrays.asList(2L, 3L), false, true, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

        verify(taskContext).offsetStorageReader();
    }

    @Test
    public void testTimestampRestoreNoVersionOffset() throws Exception {
        final TimestampIncrementingOffset offset = new TimestampIncrementingOffset(new Timestamp(10L), null);
        testTimestampRestoreOffset(Collections.singletonMap(SINGLE_TABLE_PARTITION, offset.toMap()));
    }

    @Test
    public void testTimestampRestoreVersionOneOffset() throws Exception {
        final TimestampIncrementingOffset offset = new TimestampIncrementingOffset(new Timestamp(10L), null);
        testTimestampRestoreOffset(
            Collections.singletonMap(SINGLE_TABLE_PARTITION_WITH_VERSION, offset.toMap())
        );
    }

    @Test
    public void testTimestampRestoreOffsetsWithMultipleProtocol() throws Exception {
        final TimestampIncrementingOffset oldOffset = new TimestampIncrementingOffset(
            new Timestamp(8L),
            null
        );
        final TimestampIncrementingOffset offset = new TimestampIncrementingOffset(new Timestamp(10L), null);
        final Map<Map<String, String>, Map<String, Object>> offsets = new HashMap<>();
        offsets.put(SINGLE_TABLE_PARTITION_WITH_VERSION, offset.toMap());
        offsets.put(SINGLE_TABLE_PARTITION, oldOffset.toMap());
        //we want to always use the offset with the latest protocol found
        testTimestampRestoreOffset(offsets);
    }

    private void testTimestampRestoreOffset(
        final Map<Map<String, String>, Map<String, Object>> offsets) throws Exception {
        expectInitialize(Arrays.asList(
            SINGLE_TABLE_PARTITION_WITH_VERSION, SINGLE_TABLE_PARTITION),
            offsets
        );


        // Timestamp is managed manually here so we can verify handling of duplicate values
        db.createTable(SINGLE_TABLE_NAME,
            "modified", "TIMESTAMP NOT NULL",
            "id", "INT");
        // id=2 will be ignored since it has the same timestamp as the initial offset.
        db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
            "id", 2);
        db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(11L), UTC_TIME_ZONE),
            "id", 3);
        db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(12L), UTC_TIME_ZONE),
            "id", 4);

        startTask("modified", null, null);

        // Effectively skips first poll
        verifyPoll(2, "id", Arrays.asList(3, 4), true, false, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

        verify(taskContext).offsetStorageReader();
    }

    @Test
    public void testTimestampAndIncrementingRestoreNoVersionOffset() throws Exception {
        final TimestampIncrementingOffset offset = new TimestampIncrementingOffset(new Timestamp(10L), 3L);
        testTimestampAndIncrementingRestoreOffset(
            Collections.singletonMap(SINGLE_TABLE_PARTITION, offset.toMap())
        );
    }

    @Test
    public void testTimestampAndIncrementingRestoreVersionOneOffset() throws Exception {
        final TimestampIncrementingOffset offset = new TimestampIncrementingOffset(new Timestamp(10L), 3L);
        testTimestampAndIncrementingRestoreOffset(
            Collections.singletonMap(SINGLE_TABLE_PARTITION_WITH_VERSION, offset.toMap())
        );
    }

    @Test
    public void testTimestampAndIncrementingRestoreOffsetsWithMultipleProtocol() throws Exception {
        final TimestampIncrementingOffset oldOffset = new TimestampIncrementingOffset(new Timestamp(10L), 2L);
        final TimestampIncrementingOffset offset = new TimestampIncrementingOffset(new Timestamp(10L), 3L);
        final Map<Map<String, String>, Map<String, Object>> offsets = new HashMap<>();
        offsets.put(SINGLE_TABLE_PARTITION_WITH_VERSION, offset.toMap());
        offsets.put(SINGLE_TABLE_PARTITION, oldOffset.toMap());
        //we want to always use the offset with the latest protocol found
        testTimestampAndIncrementingRestoreOffset(offsets);
    }

    private void testTimestampAndIncrementingRestoreOffset(
        final Map<Map<String, String>, Map<String, Object>> offsets) throws Exception {
        expectInitialize(Arrays.asList(
            SINGLE_TABLE_PARTITION_WITH_VERSION, SINGLE_TABLE_PARTITION),
            offsets
        );

        // Timestamp is managed manually here so we can verify handling of duplicate values
        db.createTable(SINGLE_TABLE_NAME,
            "modified", "TIMESTAMP NOT NULL",
            "id", "INT NOT NULL");
        // id=3 will be ignored since it has the same timestamp + id as the initial offset, rest
        // should be included, including id=1 which is an old ID with newer timestamp
        db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(9L), UTC_TIME_ZONE),
            "id", 2);
        db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
            "id", 3);
        db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(11L), UTC_TIME_ZONE),
            "id", 4);
        db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(12L), UTC_TIME_ZONE),
            "id", 5);
        db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(13L), UTC_TIME_ZONE),
            "id", 1);

        startTask("modified", "id", null);

        verifyPoll(3, "id", Arrays.asList(4, 5, 1), true, true, false, TOPIC_PREFIX + SINGLE_TABLE_NAME);

        verify(taskContext).offsetStorageReader();
    }


    @Test
    public void testCustomQueryBulk() throws Exception {
        db.createTable(JOIN_TABLE_NAME, "user_id", "INT", "name", "VARCHAR(64)");
        db.insert(JOIN_TABLE_NAME, "user_id", 1, "name", "Alice");
        db.insert(JOIN_TABLE_NAME, "user_id", 2, "name", "Bob");

        // Manage these manually so we can verify the emitted values
        db.createTable(SINGLE_TABLE_NAME,
            "id", "INT",
            "user_id", "INT");
        db.insert(SINGLE_TABLE_NAME, "id", 1, "user_id", 1);

        startTask(null, null, "SELECT \"test\".\"id\", \"test\""
            + ".\"user_id\", \"users\".\"name\" FROM \"test\" JOIN \"users\" "
            + "ON (\"test\".\"user_id\" = \"users\".\"user_id\")");

        List<SourceRecord> records = task.poll();
        assertThat(records).hasSize(1);
        Map<Integer, Integer> recordUserIdCounts = new HashMap<>();
        recordUserIdCounts.put(1, 1);
        assertThat(countIntValues(records, "id")).isEqualTo(recordUserIdCounts);
        assertRecordsTopic(records, TOPIC_PREFIX);
        assertRecordsSourcePartition(records, QUERY_SOURCE_PARTITION);

        db.insert(SINGLE_TABLE_NAME, "id", 2, "user_id", 1);
        db.insert(SINGLE_TABLE_NAME, "id", 3, "user_id", 2);
        db.insert(SINGLE_TABLE_NAME, "id", 4, "user_id", 2);

        records = pollRecords(task);
        assertThat(records).hasSize(4);
        recordUserIdCounts = new HashMap<>();
        recordUserIdCounts.put(1, 2);
        recordUserIdCounts.put(2, 2);
        assertThat(countIntValues(records, "user_id")).isEqualTo(recordUserIdCounts);
        assertRecordsTopic(records, TOPIC_PREFIX);
        assertRecordsSourcePartition(records, QUERY_SOURCE_PARTITION);
    }

    @Test
    public void testCustomQueryWithTimestamp() throws Exception {
        expectInitializeNoOffsets(List.of(JOIN_QUERY_PARTITION));

        db.createTable(JOIN_TABLE_NAME, "user_id", "INT", "name", "VARCHAR(64)");
        db.insert(JOIN_TABLE_NAME, "user_id", 1, "name", "Alice");
        db.insert(JOIN_TABLE_NAME, "user_id", 2, "name", "Bob");

        // Manage these manually so we can verify the emitted values
        db.createTable(SINGLE_TABLE_NAME,
            "modified", "TIMESTAMP NOT NULL",
            "id", "INT",
            "user_id", "INT");
        db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
            "id", 1,
            "user_id", 1);

        startTask("modified", null, "SELECT \"test\".\"modified\", \"test\".\"id\", \"test\""
            + ".\"user_id\", \"users\".\"name\" FROM \"test\" JOIN \"users\" "
            + "ON (\"test\".\"user_id\" = \"users\".\"user_id\")");

        verifyTimestampFirstPoll(TOPIC_PREFIX);

        db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(10L), UTC_TIME_ZONE),
            "id", 2,
            "user_id", 1);
        db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(11L), UTC_TIME_ZONE),
            "id", 3,
            "user_id", 2);
        db.insert(SINGLE_TABLE_NAME,
            "modified", DateTimeUtils.formatTimestamp(new Timestamp(12L), UTC_TIME_ZONE),
            "id", 4,
            "user_id", 2);

        verifyPoll(2, "id", Arrays.asList(3, 4), true, false, false, TOPIC_PREFIX);
    }

    private void startTask(final String timestampColumn, final String incrementingColumn, final String query) {
        startTask(timestampColumn, incrementingColumn, query, 0L, "UTC", null, null);
    }

    private void startTask(final String timestampColumn, final String incrementingColumn,
                           final String query, final Long delay, final String timeZone,
                           final Long incrementingInitial, final Long timestampInitialMs) {
        final String mode = mode(timestampColumn, incrementingColumn);
        initializeTask();
        final Map<String, String> taskConfig = taskConfig(timestampColumn, incrementingColumn,
                query, delay, timeZone,
                incrementingInitial, timestampInitialMs,
                mode);
        task.start(taskConfig);
    }

    private String mode(final String timestampColumn, final String incrementingColumn) {
        final String mode;
        if (timestampColumn != null && incrementingColumn != null) {
            mode = JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING;
        } else if (timestampColumn != null) {
            mode = JdbcSourceConnectorConfig.MODE_TIMESTAMP;
        } else if (incrementingColumn != null) {
            mode = JdbcSourceConnectorConfig.MODE_INCREMENTING;
        } else {
            mode = JdbcSourceConnectorConfig.MODE_BULK;
        }
        return mode;
    }

    private Map<String, String> taskConfig(final String timestampColumn, final String incrementingColumn,
                                           final String query, final Long delay, final String timeZone,
                                           final Long incrementingInitial, final Long timestampInitialMs,
                                           final String mode) {
        final Map<String, String> taskConfig = singleTableConfig();
        taskConfig.put(JdbcSourceConnectorConfig.MODE_CONFIG, mode);
        if (query != null) {
            taskConfig.put(JdbcSourceTaskConfig.QUERY_CONFIG, query);
            taskConfig.put(JdbcSourceTaskConfig.TABLES_CONFIG, "");
        }
        if (timestampColumn != null) {
            taskConfig.put(JdbcSourceConnectorConfig.TIMESTAMP_COLUMN_NAME_CONFIG, timestampColumn);
        }
        if (incrementingColumn != null) {
            taskConfig.put(JdbcSourceConnectorConfig.INCREMENTING_COLUMN_NAME_CONFIG, incrementingColumn);
        }
        if (incrementingInitial != null) {
            taskConfig.put(JdbcSourceConnectorConfig.INCREMENTING_INITIAL_VALUE_CONFIG, incrementingInitial.toString());
        }
        if (timestampInitialMs != null) {
            taskConfig.put(JdbcSourceConnectorConfig.TIMESTAMP_INITIAL_MS_CONFIG, timestampInitialMs.toString());
        }

        taskConfig.put(
            JdbcSourceConnectorConfig.TIMESTAMP_DELAY_INTERVAL_MS_CONFIG, delay == null ? "0" : delay.toString());
        if (timeZone != null) {
            taskConfig.put(JdbcConfig.DB_TIMEZONE_CONFIG, timeZone);
        }

        taskConfig.put(JdbcSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, "100");
        return taskConfig;
    }

    private void verifyIncrementingFirstPoll(final String topic) throws Exception {
        final List<SourceRecord> records = task.poll();
        assertThat(countIntValues(records, "id")).isEqualTo(Collections.singletonMap(1, 1));
        assertThat(countIntIncrementingOffsets(records, "id")).isEqualTo(Collections.singletonMap(1L, 1));
        assertIncrementingOffsets(records);
        assertRecordsTopic(records, topic);
    }

    private List<SourceRecord> verifyMultiTimestampFirstPoll(final String topic) throws Exception {
        final List<SourceRecord> records = task.poll();
        assertThat(records).hasSize(1);
        assertThat(countIntValues(records, "id")).containsExactly(entry(1, 1));
        assertThat(countTimestampValues(records, "created")).containsExactly(entry(10L, 1));
        assertMultiTimestampOffsets(records);
        assertRecordsTopic(records, topic);
        return records;
    }

    private List<SourceRecord> verifyTimestampFirstPoll(final String topic) throws Exception {
        final List<SourceRecord> records = task.poll();
        assertThat(records).hasSize(1);
        assertThat(countIntValues(records, "id")).containsExactly(entry(1, 1));
        assertThat(countTimestampValues(records, "modified")).containsExactly(entry(10L, 1));
        assertTimestampOffsets(records);
        assertRecordsTopic(records, topic);
        return records;
    }

    private void verifyIncrementingAndTimestampFirstPoll(final String topic) throws Exception {
        final List<SourceRecord> records = verifyTimestampFirstPoll(topic);
        assertIncrementingOffsets(records);
    }

    private void verifyIncrementingAndMultiTimestampFirstPoll(final String topic) throws Exception {
        final List<SourceRecord> records = verifyMultiTimestampFirstPoll(topic);
        assertIncrementingOffsets(records);
    }

    private <T> void verifyPoll(final int numRecords,
                                final String valueField,
                                final List<T> values,
                                final boolean timestampOffsets,
                                final boolean incrementingOffsets,
                                final boolean multiTimestampOffsets,
                                final String topic)
        throws Exception {
        List<SourceRecord> records = null;
        // May need to retry polling occasionally
        for (int retries = 0; retries < 10 && records == null; retries++) {
            records = task.poll();
        }
        assertThat(records).hasSize(numRecords);

        final HashMap<T, Integer> valueCounts = new HashMap<>();
        for (final T value : values) {
            valueCounts.put(value, 1);
        }
        assertThat(countIntValues(records, valueField)).isEqualTo(valueCounts);

        if (timestampOffsets) {
            assertTimestampOffsets(records);
        }
        if (incrementingOffsets) {
            assertIncrementingOffsets(records);
        }
        if (multiTimestampOffsets) {
            assertMultiTimestampOffsets(records);
        }

        assertRecordsTopic(records, topic);
    }

    private enum Field {
        KEY,
        VALUE,
        TIMESTAMP_VALUE,
        INCREMENTING_OFFSET,
        TIMESTAMP_OFFSET
    }

    @SuppressWarnings("unchecked")
    private <T> Map<T, Integer> countInts(final List<SourceRecord> records, final Field field, final String fieldName) {
        final Map<T, Integer> result = new HashMap<>();
        for (final SourceRecord record : records) {
            final T extracted;
            switch (field) {
                case KEY:
                    extracted = (T) record.key();
                    break;
                case VALUE:
                    extracted = (T) ((Struct) record.value()).get(fieldName);
                    break;
                case TIMESTAMP_VALUE: {
                    final java.util.Date rawTimestamp = (java.util.Date) ((Struct) record.value()).get(fieldName);
                    extracted = (T) (Long) rawTimestamp.getTime();
                    break;
                }
                case INCREMENTING_OFFSET: {
                    final TimestampIncrementingOffset offset =
                        TimestampIncrementingOffset.fromMap(record.sourceOffset());
                    extracted = (T) offset.getIncrementingOffset();
                    break;
                }
                case TIMESTAMP_OFFSET: {
                    final TimestampIncrementingOffset offset =
                        TimestampIncrementingOffset.fromMap(record.sourceOffset());
                    final Timestamp rawTimestamp = offset.getTimestampOffset();
                    extracted = (T) (Long) rawTimestamp.getTime();
                    break;
                }
                default:
                    throw new RuntimeException("Invalid field");
            }
            Integer count = result.get(extracted);
            count = (count != null ? count : 0) + 1;
            result.put(extracted, count);
        }
        return result;
    }

    private Map<Integer, Integer> countIntValues(final List<SourceRecord> records, final String fieldName) {
        return countInts(records, Field.VALUE, fieldName);
    }

    private Map<Long, Integer> countTimestampValues(final List<SourceRecord> records, final String fieldName) {
        return countInts(records, Field.TIMESTAMP_VALUE, fieldName);
    }

    private Map<Long, Integer> countIntIncrementingOffsets(final List<SourceRecord> records, final String fieldName) {
        return countInts(records, Field.INCREMENTING_OFFSET, fieldName);
    }


    private void assertIncrementingOffsets(final List<SourceRecord> records) {
        // Should use incrementing field as offsets
        for (final SourceRecord record : records) {
            final Object incrementing = ((Struct) record.value()).get("id");
            final long incrementingValue = incrementing instanceof Integer ? (long) (Integer) incrementing
                : (Long) incrementing;
            final long offsetValue = TimestampIncrementingOffset.fromMap(record.sourceOffset()).getIncrementingOffset();
            assertThat(offsetValue).isEqualTo(incrementingValue);
        }
    }

    private void assertTimestampOffsets(final List<SourceRecord> records) {
        // Should use timestamps as offsets
        for (final SourceRecord record : records) {
            final Timestamp timestampValue = (Timestamp) ((Struct) record.value()).get("modified");
            final Timestamp offsetValue =
                TimestampIncrementingOffset.fromMap(record.sourceOffset()).getTimestampOffset();
            assertThat(offsetValue).isEqualTo(timestampValue);
        }
    }

    private void assertMultiTimestampOffsets(final List<SourceRecord> records) {
        for (final SourceRecord record : records) {
            Timestamp timestampValue = (Timestamp) ((Struct) record.value()).get("modified");
            if (timestampValue == null) {
                timestampValue = (Timestamp) ((Struct) record.value()).get("created");
            }
            final Timestamp offsetValue =
                TimestampIncrementingOffset.fromMap(record.sourceOffset()).getTimestampOffset();
            assertThat(offsetValue).isEqualTo(timestampValue);
        }
    }

    private void assertRecordsTopic(final List<SourceRecord> records, final String topic) {
        for (final SourceRecord record : records) {
            assertThat(record.topic()).isEqualTo(topic);
        }
    }

    private void assertRecordsSourcePartition(final List<SourceRecord> records,
                                              final Map<String, String> partition) {
        for (final SourceRecord record : records) {
            assertThat(record.sourcePartition()).isEqualTo(partition);
        }
    }
}
