/**
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
 **/

package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class TimestampIncrementingTableQuerierTest {

  @Test
  public void extractIntOffset() throws SQLException {
    final Schema schema = SchemaBuilder.struct().field("id", SchemaBuilder.INT32_SCHEMA).build();
    final Struct record = new Struct(schema).put("id", 42);
    assertEquals(42L, newQuerier().extractOffset(schema, record).getIncrementingOffset());
  }

  @Test
  public void extractLongOffset() throws SQLException {
    final Schema schema = SchemaBuilder.struct().field("id", SchemaBuilder.INT64_SCHEMA).build();
    final Struct record = new Struct(schema).put("id", 42L);
    assertEquals(42L, newQuerier().extractOffset(schema, record).getIncrementingOffset());
  }

  @Test
  public void extractDecimalOffset() throws SQLException {
    final Schema decimalSchema = Decimal.schema(0);
    final Schema schema = SchemaBuilder.struct().field("id", decimalSchema).build();
    final Struct record = new Struct(schema).put("id", new BigDecimal(42));
    assertEquals(42L, newQuerier().extractOffset(schema, record).getIncrementingOffset());
  }

  @Test(expected = ConnectException.class)
  public void extractTooLargeDecimalOffset() throws SQLException {
    final Schema decimalSchema = Decimal.schema(0);
    final Schema schema = SchemaBuilder.struct().field("id", decimalSchema).build();
    final Struct record = new Struct(schema).put("id", new BigDecimal(Long.MAX_VALUE).add(new BigDecimal(1)));
    newQuerier().extractOffset(schema, record).getIncrementingOffset();
  }

  @Test(expected = ConnectException.class)
  public void extractFractionalDecimalOffset() throws SQLException {
    final Schema decimalSchema = Decimal.schema(2);
    final Schema schema = SchemaBuilder.struct().field("id", decimalSchema).build();
    final Struct record = new Struct(schema).put("id", new BigDecimal("42.42"));
    newQuerier().extractOffset(schema, record).getIncrementingOffset();
  }

  private TimestampIncrementingTableQuerier newQuerier() {
    return new TimestampIncrementingTableQuerier(TableQuerier.QueryMode.TABLE, null, "", null, "id", Collections.<String, Object>emptyMap(), 0L, null);
  }

}
