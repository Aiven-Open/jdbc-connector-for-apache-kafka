/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.connect.jdbc.sink.JdbcSinkConfig;

import com.google.common.collect.Lists;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FieldsMetadataTest {

    private static final Schema SIMPLE_PRIMITIVE_SCHEMA = Schema.INT64_SCHEMA;
    private static final Schema SIMPLE_STRUCT_SCHEMA = SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA).build();
    private static final Schema SIMPLE_MAP_SCHEMA = SchemaBuilder.map(SchemaBuilder.INT64_SCHEMA, Schema.STRING_SCHEMA);

    @Test(expected = ConnectException.class)
    public void valueSchemaMustBePresentForPkModeRecordValue() {
        extract(
            JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE,
            Collections.emptyList(),
            SIMPLE_PRIMITIVE_SCHEMA,
            null
        );
    }

    @Test(expected = ConnectException.class)
    public void valueSchemaMustBeStructIfPresent() {
        extract(
            JdbcSinkConfig.PrimaryKeyMode.KAFKA,
            Collections.emptyList(),
            SIMPLE_PRIMITIVE_SCHEMA,
            SIMPLE_PRIMITIVE_SCHEMA
        );
    }

    @Test
    public void missingValueSchemaCanBeOk() {
        assertThat(extract(
            JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY,
            Collections.emptyList(),
            SIMPLE_STRUCT_SCHEMA,
            null
        ).allFields.keySet()).isEqualTo(Set.of("name"));

        // this one is a bit weird, only columns being inserted would be kafka coords...
        // but not sure should explicitly disallow!
        assertThat(Lists.newArrayList(extract(
            JdbcSinkConfig.PrimaryKeyMode.KAFKA,
            Collections.emptyList(),
            null,
            null
        ).allFields.keySet())).isEqualTo(List.of("__connect_topic", "__connect_partition", "__connect_offset"));
    }

    @Test(expected = ConnectException.class)
    public void metadataMayNotBeEmpty() {
        extract(
            JdbcSinkConfig.PrimaryKeyMode.NONE,
            Collections.emptyList(),
            null,
            null
        );
    }

    @Test
    public void kafkaPkMode() {
        final FieldsMetadata metadata = extract(
            JdbcSinkConfig.PrimaryKeyMode.KAFKA,
            Collections.emptyList(),
            null,
            SIMPLE_STRUCT_SCHEMA
        );
        assertThat(Lists.newArrayList(metadata.keyFieldNames)).isEqualTo(
            List.of("__connect_topic", "__connect_partition", "__connect_offset"));
        assertThat(metadata.nonKeyFieldNames).isEqualTo(Set.of("name"));

        final SinkRecordField topicField = metadata.allFields.get("__connect_topic");
        assertThat(topicField.schemaType()).isEqualTo(Schema.Type.STRING);
        assertThat(topicField.isPrimaryKey()).isTrue();
        assertThat(topicField.isOptional()).isFalse();

        final SinkRecordField partitionField = metadata.allFields.get("__connect_partition");
        assertThat(partitionField.schemaType()).isEqualTo(Schema.Type.INT32);
        assertThat(partitionField.isPrimaryKey()).isTrue();
        assertThat(partitionField.isOptional()).isFalse();

        final SinkRecordField offsetField = metadata.allFields.get("__connect_offset");
        assertThat(offsetField.schemaType()).isEqualTo(Schema.Type.INT64);
        assertThat(offsetField.isPrimaryKey()).isTrue();
        assertThat(offsetField.isOptional()).isFalse();
    }

    @Test
    public void kafkaPkModeCustomNames() {
        final List<String> customKeyNames = List.of("the_topic", "the_partition", "the_offset");
        final FieldsMetadata metadata = extract(
            JdbcSinkConfig.PrimaryKeyMode.KAFKA,
            customKeyNames,
            null,
            SIMPLE_STRUCT_SCHEMA
        );
        assertThat(Lists.newArrayList(metadata.keyFieldNames)).isEqualTo(customKeyNames);
        assertThat(metadata.nonKeyFieldNames).isEqualTo(Collections.singleton("name"));
    }

    @Test(expected = ConnectException.class)
    public void kafkaPkModeBadFieldSpec() {
        extract(
            JdbcSinkConfig.PrimaryKeyMode.KAFKA,
            List.of("lone"),
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
            List.of("the_pk"),
            SIMPLE_PRIMITIVE_SCHEMA,
            SIMPLE_STRUCT_SCHEMA
        );

        assertThat(metadata.keyFieldNames).isEqualTo(Collections.singleton("the_pk"));

        assertThat(metadata.nonKeyFieldNames).isEqualTo(Collections.singleton("name"));

        assertThat(metadata.allFields.get("the_pk").schemaType()).isEqualTo(SIMPLE_PRIMITIVE_SCHEMA.type());
        assertThat(metadata.allFields.get("the_pk").isPrimaryKey()).isTrue();
        assertThat(metadata.allFields.get("the_pk").isOptional()).isFalse();

        assertThat(metadata.allFields.get("name").schemaType()).isEqualTo(Schema.Type.STRING);
        assertThat(metadata.allFields.get("name").isPrimaryKey()).isFalse();
        assertThat(metadata.allFields.get("name").isOptional()).isFalse();
    }

    @Test(expected = ConnectException.class)
    public void recordKeyPkModeWithPrimitiveKeyButMultiplePkFieldsSpecified() {
        extract(
            JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY,
            List.of("pk1", "pk2"),
            SIMPLE_PRIMITIVE_SCHEMA,
            SIMPLE_STRUCT_SCHEMA
        );
    }

    @Test(expected = ConnectException.class)
    public void recordKeyPkModeButKeySchemaMissing() {
        extract(
            JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY,
            Collections.emptyList(),
            null,
            SIMPLE_STRUCT_SCHEMA
        );
    }

    @Test(expected = ConnectException.class)
    public void recordKeyPkModeButKeySchemaAsNonStructCompositeType() {
        extract(
            JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY,
            Collections.emptyList(),
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
            List.of("nonexistent"),
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

        assertThat(metadata.keyFieldNames).isEqualTo(Set.of("name"));
        assertThat(metadata.nonKeyFieldNames).isEqualTo(Collections.emptySet());

        assertThat(metadata.allFields.get("name").schemaType()).isEqualTo(Schema.Type.STRING);
        assertThat(metadata.allFields.get("name").isPrimaryKey()).isTrue();
        assertThat(metadata.allFields.get("name").isOptional()).isFalse();
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
            Set.of("field2", "field4"),
            null,
            valueSchema
        );

        assertThat(Lists.newArrayList(metadata.keyFieldNames)).isEqualTo(List.of("field1"));
        assertThat(Lists.newArrayList(metadata.nonKeyFieldNames)).isEqualTo(List.of("field2", "field4"));
    }

    @Test
    public void recordValuePkModeWithFieldsInOriginalOrdering() {
        final Schema valueSchema =
                SchemaBuilder.struct()
                        .field("field4", Schema.INT64_SCHEMA)
                        .field("field2", Schema.INT64_SCHEMA)
                        .field("field1", Schema.INT64_SCHEMA)
                        .field("field3", Schema.INT64_SCHEMA)
                        .build();

        var metadata = extract(
                JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE,
                Collections.singletonList("field4"),
                Set.of("field3", "field1", "field2"),
                null,
                valueSchema
        );

        assertThat(Lists.newArrayList(metadata.allFields.keySet())).isEqualTo(
            List.of("field4", "field2", "field1", "field3"));

        metadata = extract(
                JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE,
                Collections.singletonList("field1"),
                Set.of("field4", "field3"),
                null,
                valueSchema
        );

        assertThat(Lists.newArrayList(metadata.allFields.keySet())).isEqualTo(List.of("field4", "field1", "field3"));

        final var keySchema =
                SchemaBuilder.struct()
                        .field("field1", Schema.INT64_SCHEMA)
                        .field("field3", Schema.INT64_SCHEMA)
                        .field("field2", Schema.INT64_SCHEMA)
                        .build();

        metadata = extract(
                JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY,
                List.of("field2", "field3", "field1"),
                Set.of("field3", "field1"),
                keySchema,
                null
        );

        assertThat(Lists.newArrayList(metadata.allFields.keySet())).isEqualTo(List.of("field1", "field2", "field3"));
    }

    private static FieldsMetadata extract(final JdbcSinkConfig.PrimaryKeyMode pkMode,
                                          final List<String> pkFields,
                                          final Schema keySchema,
                                          final Schema valueSchema) {
        return extract(pkMode, pkFields, Collections.emptySet(), keySchema, valueSchema);
    }

    private static FieldsMetadata extract(final JdbcSinkConfig.PrimaryKeyMode pkMode,
                                          final List<String> pkFields,
                                          final Set<String> whitelist,
                                          final Schema keySchema,
                                          final Schema valueSchema) {
        return FieldsMetadata.extract("table", pkMode, pkFields, whitelist, keySchema, valueSchema);
    }
}
