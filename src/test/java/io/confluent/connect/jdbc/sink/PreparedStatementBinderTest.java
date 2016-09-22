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

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import org.mockito.internal.verification.Times;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PreparedStatementBinderTest {

  @Test
  public void bindRecord() throws SQLException, ParseException {
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
        .field("decimal", Decimal.schema(0))
        .field("date", Date.SCHEMA)
        .field("time", Time.SCHEMA)
        .field("timestamp", Timestamp.SCHEMA)
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
        .put("age", 30)
        .put("decimal", new BigDecimal("1.5").setScale(0, BigDecimal.ROUND_HALF_EVEN))
        .put("date", new java.util.Date(0))
        .put("time", new java.util.Date(1000))
        .put("timestamp", new java.util.Date(100));

    SchemaPair schemaPair = new SchemaPair(null, valueSchema);

    JdbcSinkConfig.PrimaryKeyMode pkMode = JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE;

    List<String> pkFields = Collections.singletonList("long");

    FieldsMetadata fieldsMetadata = FieldsMetadata.extract("people", pkMode, pkFields, Collections.<String>emptySet(), schemaPair);

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
    verify(statement, times(1)).setBigDecimal(index++, (BigDecimal) valueStruct.get("decimal"));
    verify(statement, times(1)).setDate(index++, new java.sql.Date(((java.util.Date) valueStruct.get("date")).getTime()));
    verify(statement, times(1)).setTime(index++, new java.sql.Time(((java.util.Date) valueStruct.get("time")).getTime()));
    verify(statement, times(1)).setTimestamp(index++, new java.sql.Timestamp(((java.util.Date) valueStruct.get("timestamp")).getTime()));
    // last field is optional and is null-valued in struct
    verify(statement, times(1)).setObject(index++, null);
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
    verifyBindField(++index, Schema.BYTES_SCHEMA, ByteBuffer.wrap(new byte[]{42})).setBytes(index, new byte[]{42});
    verifyBindField(++index, Schema.STRING_SCHEMA, "yep").setString(index, "yep");
    verifyBindField(++index, Decimal.schema(0), new BigDecimal("1.5").setScale(0, BigDecimal.ROUND_HALF_EVEN)).setBigDecimal(index, new BigDecimal(2));
    verifyBindField(++index, Date.SCHEMA, new java.util.Date(0)).setDate(index, new java.sql.Date(0));
    verifyBindField(++index, Time.SCHEMA, new java.util.Date(1000)).setTime(index, new java.sql.Time(1000));
    verifyBindField(++index, Timestamp.SCHEMA, new java.util.Date(100)).setTimestamp(index, new java.sql.Timestamp(100));
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
    for (Schema schema : nullableTypes) {
      verifyBindField(++index, schema, null).setObject(index, null);
    }
  }

  @Test(expected = ConnectException.class)
  public void bindFieldStructUnsupported() throws SQLException {
    Schema structSchema = SchemaBuilder.struct().field("test", Schema.BOOLEAN_SCHEMA).build();
    PreparedStatementBinder.bindField(mock(PreparedStatement.class), 1, structSchema, new Struct(structSchema));
  }

  @Test(expected = ConnectException.class)
  public void bindFieldArrayUnsupported() throws SQLException {
    Schema arraySchema = SchemaBuilder.array(Schema.INT8_SCHEMA);
    PreparedStatementBinder.bindField(mock(PreparedStatement.class), 1, arraySchema, Collections.emptyList());
  }

  @Test(expected = ConnectException.class)
  public void bindFieldMapUnsupported() throws SQLException {
    Schema mapSchema = SchemaBuilder.map(Schema.INT8_SCHEMA, Schema.INT8_SCHEMA);
    PreparedStatementBinder.bindField(mock(PreparedStatement.class), 1, mapSchema, Collections.emptyMap());
  }

  private PreparedStatement verifyBindField(int index, Schema schema, Object value) throws SQLException {
    PreparedStatement statement = mock(PreparedStatement.class);
    PreparedStatementBinder.bindField(statement, index, schema, value);
    return verify(statement, times(1));
  }

}
