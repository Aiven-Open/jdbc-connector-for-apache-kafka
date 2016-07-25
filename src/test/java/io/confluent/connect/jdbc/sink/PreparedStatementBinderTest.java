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

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PreparedStatementBinderTest {

  @Test
  public void bindRecord() throws SQLException {
    Schema valueSchema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("bool", Schema.BOOLEAN_SCHEMA)
        .field("short", Schema.INT16_SCHEMA)
        .field("byte", Schema.INT8_SCHEMA)
        .field("long", Schema.INT64_SCHEMA)
        .field("float", Schema.FLOAT32_SCHEMA)
        .field("double", Schema.FLOAT64_SCHEMA)
        .field("bytes", Schema.BYTES_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA)
        .build();

    Struct valueStruct = new Struct(valueSchema)
        .put("firstName", "Alex")
        .put("lastName", "Smith")
        .put("bool", true)
        .put("short", (short) 1234)
        .put("byte", (byte) -32)
        .put("long", (long) 12425436)
        .put("float", (float) 2356.3)
        .put("double", -2436546.56457)
        .put("bytes", new byte[]{-32, 124})
        .put("age", 30);

    SchemaPair schemaPair = new SchemaPair(null, valueSchema);

    JdbcSinkConfig.PrimaryKeyMode pkMode = JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE;

    List<String> pkFields = Collections.singletonList("long");

    FieldsMetadata fieldsMetadata = FieldsMetadata.extract("people", pkMode, pkFields, schemaPair);

    PreparedStatement statement = mock(PreparedStatement.class);

    PreparedStatementBinder binder = new PreparedStatementBinder(
        statement,
        pkMode,
        schemaPair,
        fieldsMetadata
    );

    binder.bindRecord(new SinkRecord("topic", 0, null, null, valueSchema, valueStruct, 0));

    int index = 1;
    // key field first
    verify(statement, times(1)).setLong(index++, valueStruct.getInt64("long"));
    // rest in order of schema def
    verify(statement, times(1)).setString(index++, valueStruct.getString("firstName"));
    verify(statement, times(1)).setString(index++, valueStruct.getString("lastName"));
    verify(statement, times(1)).setInt(index++, valueStruct.getInt32("age"));
    verify(statement, times(1)).setBoolean(index++, valueStruct.getBoolean("bool"));
    verify(statement, times(1)).setShort(index++, valueStruct.getInt16("short"));
    verify(statement, times(1)).setByte(index++, valueStruct.getInt8("byte"));
    verify(statement, times(1)).setFloat(index++, valueStruct.getFloat32("float"));
    verify(statement, times(1)).setDouble(index++, valueStruct.getFloat64("double"));
    verify(statement, times(1)).setBytes(index++, valueStruct.getBytes("bytes"));
    // last field is optional and is null-valued in struct
    verify(statement, times(1)).setObject(index++, null);
  }


  @Test
  public void bindFieldPrimitiveValues() throws SQLException {
    int index = ThreadLocalRandom.current().nextInt();
    verifyBindField(++index, Schema.Type.INT8, (byte) 42).setByte(index, (byte) 42);
    verifyBindField(++index, Schema.Type.INT16, (short) 42).setShort(index, (short) 42);
    verifyBindField(++index, Schema.Type.INT32, 42).setInt(index, 42);
    verifyBindField(++index, Schema.Type.INT64, 42L).setLong(index, 42L);
    verifyBindField(++index, Schema.Type.BOOLEAN, false).setBoolean(index, false);
    verifyBindField(++index, Schema.Type.BOOLEAN, true).setBoolean(index, true);
    verifyBindField(++index, Schema.Type.FLOAT32, -42f).setFloat(index, -42f);
    verifyBindField(++index, Schema.Type.FLOAT64, 42d).setDouble(index, 42d);
    verifyBindField(++index, Schema.Type.BYTES, new byte[]{42}).setBytes(index, new byte[]{42});
    verifyBindField(++index, Schema.Type.BYTES, ByteBuffer.wrap(new byte[]{42})).setBytes(index, new byte[]{42});
    verifyBindField(++index, Schema.Type.STRING, "yep").setString(index, "yep");
  }

  @Test
  public void bindFieldNull() throws SQLException {
    final List<Schema.Type> nullableTypes = Arrays.asList(
        Schema.Type.INT8,
        Schema.Type.INT16,
        Schema.Type.INT32,
        Schema.Type.INT64,
        Schema.Type.FLOAT32,
        Schema.Type.FLOAT64,
        Schema.Type.BOOLEAN,
        Schema.Type.BYTES,
        Schema.Type.STRING
    );
    int index = 0;
    for (Schema.Type type : nullableTypes) {
      verifyBindField(++index, type, null).setObject(index, null);
    }
  }

  @Test(expected = ConnectException.class)
  public void bindFieldStructUnsupported() throws SQLException {
    Schema structSchema = SchemaBuilder.struct().field("test", Schema.BOOLEAN_SCHEMA).build();
    PreparedStatementBinder.bindField(mock(PreparedStatement.class), 1, Schema.Type.STRUCT, new Struct(structSchema));
  }

  @Test(expected = ConnectException.class)
  public void bindFieldArrayUnsupported() throws SQLException {
    PreparedStatementBinder.bindField(mock(PreparedStatement.class), 1, Schema.Type.ARRAY, Collections.emptyList());
  }

  @Test(expected = ConnectException.class)
  public void bindFieldMapUnsupported() throws SQLException {
    PreparedStatementBinder.bindField(mock(PreparedStatement.class), 1, Schema.Type.MAP, Collections.emptyMap());
  }

  private PreparedStatement verifyBindField(int index, Schema.Type schemaType, Object value) throws SQLException {
    PreparedStatement statement = mock(PreparedStatement.class);
    PreparedStatementBinder.bindField(statement, index, schemaType, value);
    return verify(statement, times(1));
  }

}
