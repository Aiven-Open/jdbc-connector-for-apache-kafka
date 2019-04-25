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

package io.aiven.connect.jdbc.sink.metadata;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.connect.jdbc.sink.JdbcSinkConfig;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FieldsMetadataTest {

    private static final Schema SIMPLE_PRIMITIVE_SCHEMA = Schema.INT64_SCHEMA;
    private static final Schema SIMPLE_STRUCT_SCHEMA = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA).build();
    private static final Schema SIMPLE_MAP_SCHEMA = SchemaBuilder.map(SchemaBuilder.INT64_SCHEMA, Schema.STRING_SCHEMA);

    @Test(expected = ConnectException.class)
    public void valueSchemaMustBePresentForPkModeRecordValue() {
        extract(
            JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE,
            Collections.<String>emptyList(),
            SIMPLE_PRIMITIVE_SCHEMA,
            null
        );
    }

    @Test(expected = ConnectException.class)
    public void valueSchemaMustBeStructIfPresent() {
        extract(
            JdbcSinkConfig.PrimaryKeyMode.KAFKA,
            Collections.<String>emptyList(),
            SIMPLE_PRIMITIVE_SCHEMA,
            SIMPLE_PRIMITIVE_SCHEMA
        );
    }

    @Test
    public void missingValueSchemaCanBeOk() {
        assertEquals(
            new HashSet<>(Collections.singletonList("name")),
            extract(
                JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY,
                Collections.<String>emptyList(),
                SIMPLE_STRUCT_SCHEMA,
                null
            ).allFields.keySet()
        );

        // this one is a bit weird, only columns being inserted would be kafka coords...
        // but not sure should explicitly disallow!
        assertEquals(
            new HashSet<>(Arrays.asList("__connect_topic", "__connect_partition", "__connect_offset")),
            extract(
                JdbcSinkConfig.PrimaryKeyMode.KAFKA,
                Collections.<String>emptyList(),
                null,
                null
            ).allFields.keySet()
        );
    }

    @Test(expected = ConnectException.class)
    public void metadataMayNotBeEmpty() {
        extract(
            JdbcSinkConfig.PrimaryKeyMode.NONE,
            Collections.<String>emptyList(),
            null,
            null
        );
    }

    @Test
    public void kafkaPkMode() {
        final FieldsMetadata metadata = extract(
            JdbcSinkConfig.PrimaryKeyMode.KAFKA,
            Collections.<String>emptyList(),
            null,
            SIMPLE_STRUCT_SCHEMA
        );
        assertEquals(
            new HashSet<>(Arrays.asList("__connect_topic", "__connect_partition", "__connect_offset")),
            metadata.keyFieldNames);
        assertEquals(Collections.singleton("name"), metadata.nonKeyFieldNames);

        final SinkRecordField topicField = metadata.allFields.get("__connect_topic");
        assertEquals(Schema.Type.STRING, topicField.schemaType());
        assertTrue(topicField.isPrimaryKey());
        assertFalse(topicField.isOptional());

        final SinkRecordField partitionField = metadata.allFields.get("__connect_partition");
        assertEquals(Schema.Type.INT32, partitionField.schemaType());
        assertTrue(partitionField.isPrimaryKey());
        assertFalse(partitionField.isOptional());

        final SinkRecordField offsetField = metadata.allFields.get("__connect_offset");
        assertEquals(Schema.Type.INT64, offsetField.schemaType());
        assertTrue(offsetField.isPrimaryKey());
        assertFalse(offsetField.isOptional());
    }

    @Test
    public void kafkaPkModeCustomNames() {
        final List<String> customKeyNames = Arrays.asList("the_topic", "the_partition", "the_offset");
        final FieldsMetadata metadata = extract(
            JdbcSinkConfig.PrimaryKeyMode.KAFKA,
            customKeyNames,
            null,
            SIMPLE_STRUCT_SCHEMA
        );
        assertEquals(new HashSet<>(customKeyNames), metadata.keyFieldNames);
        assertEquals(Collections.singleton("name"), metadata.nonKeyFieldNames);
    }

    @Test(expected = ConnectException.class)
    public void kafkaPkModeBadFieldSpec() {
        extract(
            JdbcSinkConfig.PrimaryKeyMode.KAFKA,
            Collections.singletonList("lone"),
            null,
            SIMPLE_STRUCT_SCHEMA
        );
    }

    /**
     * RECORD_KEY test cases:
     * if keySchema is a struct, pkCols must be a subset of the keySchema fields
     */

    @Test
    public void recordKeyPkModePrimitiveKey() {
        final FieldsMetadata metadata = extract(
            JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY,
            Collections.singletonList("the_pk"),
            SIMPLE_PRIMITIVE_SCHEMA,
            SIMPLE_STRUCT_SCHEMA
        );

        assertEquals(Collections.singleton("the_pk"), metadata.keyFieldNames);

        assertEquals(Collections.singleton("name"), metadata.nonKeyFieldNames);

        assertEquals(SIMPLE_PRIMITIVE_SCHEMA.type(), metadata.allFields.get("the_pk").schemaType());
        assertTrue(metadata.allFields.get("the_pk").isPrimaryKey());
        assertFalse(metadata.allFields.get("the_pk").isOptional());

        assertEquals(Schema.Type.STRING, metadata.allFields.get("name").schemaType());
        assertFalse(metadata.allFields.get("name").isPrimaryKey());
        assertFalse(metadata.allFields.get("name").isOptional());
    }

    @Test(expected = ConnectException.class)
    public void recordKeyPkModeWithPrimitiveKeyButMultiplePkFieldsSpecified() {
        extract(
            JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY,
            Arrays.asList("pk1", "pk2"),
            SIMPLE_PRIMITIVE_SCHEMA,
            SIMPLE_STRUCT_SCHEMA
        );
    }

    @Test(expected = ConnectException.class)
    public void recordKeyPkModeButKeySchemaMissing() {
        extract(
            JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY,
            Collections.<String>emptyList(),
            null,
            SIMPLE_STRUCT_SCHEMA
        );
    }

    @Test(expected = ConnectException.class)
    public void recordKeyPkModeButKeySchemaAsNonStructCompositeType() {
        extract(
            JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY,
            Collections.<String>emptyList(),
            SIMPLE_MAP_SCHEMA,
            SIMPLE_STRUCT_SCHEMA
        );
    }

    @Test(expected = ConnectException.class)
    public void recordKeyPkModeWithStructKeyButMissingField() {
        extract(
            JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY,
            Collections.singletonList("nonexistent"),
            SIMPLE_STRUCT_SCHEMA,
            SIMPLE_STRUCT_SCHEMA
        );
    }

    @Test(expected = ConnectException.class)
    public void recordValuePkModeWithMissingPkField() {
        extract(
            JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE,
            Collections.singletonList("nonexistent"),
            SIMPLE_PRIMITIVE_SCHEMA,
            SIMPLE_STRUCT_SCHEMA
        );
    }

    @Test
    public void recordValuePkModeWithValidPkFields() {
        final FieldsMetadata metadata = extract(
            JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE,
            Collections.singletonList("name"),
            SIMPLE_PRIMITIVE_SCHEMA,
            SIMPLE_STRUCT_SCHEMA
        );

        assertEquals(Collections.singleton("name"), metadata.keyFieldNames);
        assertEquals(Collections.emptySet(), metadata.nonKeyFieldNames);

        assertEquals(Schema.Type.STRING, metadata.allFields.get("name").schemaType());
        assertTrue(metadata.allFields.get("name").isPrimaryKey());
        assertFalse(metadata.allFields.get("name").isOptional());
    }

    @Test
    public void recordValuePkModeWithPkFieldsAndWhitelistFiltering() {
        final Schema valueSchema =
            SchemaBuilder.struct()
                .field("field1", Schema.INT64_SCHEMA)
                .field("field2", Schema.INT64_SCHEMA)
                .field("field3", Schema.INT64_SCHEMA)
                .field("field4", Schema.INT64_SCHEMA)
                .build();

        final FieldsMetadata metadata = extract(
            JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE,
            Collections.singletonList("field1"),
            new HashSet<>(Arrays.asList("field2", "field4")),
            null,
            valueSchema
        );

        assertEquals(Collections.singleton("field1"), metadata.keyFieldNames);
        assertEquals(new HashSet<>(Arrays.asList("field2", "field4")), metadata.nonKeyFieldNames);
    }

    private static FieldsMetadata extract(final JdbcSinkConfig.PrimaryKeyMode pkMode,
                                          final List<String> pkFields,
                                          final Schema keySchema,
                                          final Schema valueSchema) {
        return extract(pkMode, pkFields, Collections.<String>emptySet(), keySchema, valueSchema);
    }

    private static FieldsMetadata extract(final JdbcSinkConfig.PrimaryKeyMode pkMode,
                                          final List<String> pkFields,
                                          final Set<String> whitelist,
                                          final Schema keySchema,
                                          final Schema valueSchema) {
        return FieldsMetadata.extract("table", pkMode, pkFields, whitelist, keySchema, valueSchema);
    }
}
