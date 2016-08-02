/*
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

package io.confluent.connect.jdbc.sink.metadata;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import io.confluent.connect.jdbc.sink.JdbcSinkConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FieldsMetadataTest {

  private static final Schema SIMPLE_PRIMITIVE_SCHEMA = Schema.INT64_SCHEMA;
  private static final Schema SIMPLE_STRUCT_SCHEMA = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
  private static final Schema SIMPLE_MAP_SCHEMA = SchemaBuilder.map(SchemaBuilder.INT64_SCHEMA, Schema.STRING_SCHEMA);

  @Test(expected = ConnectException.class)
  public void valueSchemaMustBePresent() {
    extract(
        JdbcSinkConfig.PrimaryKeyMode.KAFKA,
        Collections.<String>emptyList(),
        SIMPLE_PRIMITIVE_SCHEMA,
        null
    );
  }

  @Test(expected = ConnectException.class)
  public void valueSchemaMustBeStruct() {
    extract(
        JdbcSinkConfig.PrimaryKeyMode.KAFKA,
        Collections.<String>emptyList(),
        SIMPLE_PRIMITIVE_SCHEMA,
        SIMPLE_PRIMITIVE_SCHEMA
    );
  }

  @Test
  public void kafkaPkMode() {
    FieldsMetadata metadata = extract(
        JdbcSinkConfig.PrimaryKeyMode.KAFKA,
        Collections.<String>emptyList(),
        null,
        SIMPLE_STRUCT_SCHEMA
    );
    assertEquals(new HashSet<>(Arrays.asList("__connect_topic", "__connect_partition", "__connect_offset")), metadata.keyFieldNames);
    assertEquals(Collections.singleton("name"), metadata.nonKeyFieldNames);

    SinkRecordField topicField = metadata.allFields.get("__connect_topic");
    assertEquals(Schema.Type.STRING, topicField.type);
    assertTrue(topicField.isPrimaryKey);
    assertFalse(topicField.isOptional);

    SinkRecordField partitionField = metadata.allFields.get("__connect_partition");
    assertEquals(Schema.Type.INT32, partitionField.type);
    assertTrue(partitionField.isPrimaryKey);
    assertFalse(partitionField.isOptional);

    SinkRecordField offsetField = metadata.allFields.get("__connect_offset");
    assertEquals(Schema.Type.INT64, offsetField.type);
    assertTrue(offsetField.isPrimaryKey);
    assertFalse(offsetField.isOptional);
  }

  @Test
  public void kafkaPkModeCustomNames() {
    List<String> customKeyNames = Arrays.asList("the_topic", "the_partition", "the_offset");
    FieldsMetadata metadata = extract(
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
    FieldsMetadata metadata = extract(
        JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY,
        Collections.singletonList("the_pk"),
        SIMPLE_PRIMITIVE_SCHEMA,
        SIMPLE_STRUCT_SCHEMA
    );

    assertEquals(Collections.singleton("the_pk"), metadata.keyFieldNames);

    assertEquals(Collections.singleton("name"), metadata.nonKeyFieldNames);

    assertEquals(SIMPLE_PRIMITIVE_SCHEMA.type(), metadata.allFields.get("the_pk").type);
    assertTrue(metadata.allFields.get("the_pk").isPrimaryKey);
    assertFalse(metadata.allFields.get("the_pk").isOptional);

    assertEquals(Schema.Type.STRING, metadata.allFields.get("name").type);
    assertFalse(metadata.allFields.get("name").isPrimaryKey);
    assertFalse(metadata.allFields.get("name").isOptional);
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
    extract(
        JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE,
        Collections.singletonList("name"),
        SIMPLE_PRIMITIVE_SCHEMA,
        SIMPLE_STRUCT_SCHEMA
    );
  }

  private static FieldsMetadata extract(JdbcSinkConfig.PrimaryKeyMode pkMode, List<String> pkFields, Schema keySchema, Schema valueSchema) {
    return FieldsMetadata.extract("table", pkMode, pkFields, keySchema, valueSchema);
  }
}
