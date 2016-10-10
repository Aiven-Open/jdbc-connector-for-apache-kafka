/*
 *  Copyright 2016 Confluent Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.confluent.connect.jdbc.sink.dialect;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Arrays;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public abstract class BaseDialectTest {

  protected static final String TABLE_NAME = "test";

  protected final DbDialect dialect;

  protected BaseDialectTest(DbDialect dialect) {
    this.dialect = dialect;
  }

  protected void verifyDataTypeMapping(String expected, Schema schema) {
    assertEquals(expected, dialect.getSqlType(schema.name(), schema.parameters(), schema.type()));
  }

  protected void verifyCreateOneColNoPk(String expected) {
    assertEquals(expected, dialect.getCreateQuery(TABLE_NAME, Arrays.asList(
        new SinkRecordField(Schema.INT32_SCHEMA, "col1", false)
    )));
  }

  protected void verifyCreateOneColOnePk(String expected) {
    assertEquals(expected, dialect.getCreateQuery(TABLE_NAME, Arrays.asList(
        new SinkRecordField(Schema.INT32_SCHEMA, "pk1", true)
    )));
  }

  protected void verifyCreateThreeColTwoPk(String expected) {
    assertEquals(expected, dialect.getCreateQuery(TABLE_NAME, Arrays.asList(
        new SinkRecordField(Schema.INT32_SCHEMA, "pk1", true),
        new SinkRecordField(Schema.INT32_SCHEMA, "pk2", true),
        new SinkRecordField(Schema.INT32_SCHEMA, "col1", false)
    )));
  }

  protected void verifyAlterAddOneCol(String... expected) {
    assertArrayEquals(expected, dialect.getAlterTable(TABLE_NAME, Arrays.asList(
        new SinkRecordField(Schema.OPTIONAL_INT32_SCHEMA, "newcol1", false)
    )).toArray());
  }

  protected void verifyAlterAddTwoCols(String... expected) {
    assertArrayEquals(expected, dialect.getAlterTable(TABLE_NAME, Arrays.asList(
        new SinkRecordField(Schema.OPTIONAL_INT32_SCHEMA, "newcol1", false),
        new SinkRecordField(SchemaBuilder.int32().defaultValue(42).build(), "newcol2", false)
    )).toArray());
  }

}
