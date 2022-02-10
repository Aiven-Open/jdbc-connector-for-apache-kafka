/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2019 Confluent Inc.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;

import io.aiven.connect.jdbc.sink.metadata.SinkRecordField;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DbStructureTest {

    DbStructure structure = new DbStructure(null);

    @Test
    public void testNoMissingFields() {
        assertTrue(missingFields(sinkRecords("aaa"), columns("aaa", "bbb")).isEmpty());
    }

    @Test
    public void testMissingFieldsWithSameCase() {
        assertEquals(1, missingFields(sinkRecords("aaa", "bbb"), columns("aaa")).size());
    }

    @Test
    public void testSameNamesDifferentCases() {
        assertTrue(missingFields(sinkRecords("aaa"), columns("aAa", "AaA")).isEmpty());
    }

    @Test
    public void testMissingFieldsWithDifferentCase() {
        assertTrue(missingFields(sinkRecords("aaa", "bbb"), columns("AaA", "BbB")).isEmpty());
        assertTrue(missingFields(sinkRecords("AaA", "bBb"), columns("aaa", "bbb")).isEmpty());
        assertTrue(missingFields(sinkRecords("AaA", "bBb"), columns("aAa", "BbB")).isEmpty());
    }

    private Set<SinkRecordField> missingFields(
        final Collection<SinkRecordField> fields,
        final Set<String> dbColumnNames
    ) {
        return structure.missingFields(fields, dbColumnNames);
    }

    static Set<String> columns(final String... names) {
        return new HashSet<>(Arrays.asList(names));
    }

    static List<SinkRecordField> sinkRecords(final String... names) {
        final List<SinkRecordField> fields = new ArrayList<>();
        for (final String n : names) {
            fields.add(field(n));
        }
        return fields;
    }

    static SinkRecordField field(final String name) {
        return new SinkRecordField(Schema.STRING_SCHEMA, name, false);
    }
}
