/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;

import static org.junit.Assert.assertEquals;

public class TimestampIncrementingCriteriaTest {

  private static final TableId TABLE_ID = new TableId(null, null,"myTable");
  private static final ColumnId INCREMENTING_COLUMN = new ColumnId(TABLE_ID, "id");
  private static final ColumnId TS1_COLUMN = new ColumnId(TABLE_ID, "ts1");
  private static final ColumnId TS2_COLUMN = new ColumnId(TABLE_ID, "ts2");
  private static final List<ColumnId> TS_COLUMNS = Arrays.asList(TS1_COLUMN, TS2_COLUMN);

  private TimestampIncrementingCriteria criteria;
  private TimestampIncrementingCriteria criteriaInc;
  private TimestampIncrementingCriteria criteriaTs;
  private TimestampIncrementingCriteria criteriaIncTs;
  private Schema schema;
  private Struct record;

  @Before
  public void beforeEach() {
    criteria = new TimestampIncrementingCriteria(null, null);
    criteriaInc = new TimestampIncrementingCriteria(INCREMENTING_COLUMN, null);
    criteriaTs = new TimestampIncrementingCriteria(null, TS_COLUMNS);
    criteriaIncTs = new TimestampIncrementingCriteria(INCREMENTING_COLUMN, TS_COLUMNS);
  }

  protected void assertExtractedOffset(long expected, Schema schema, Struct record) {
    TimestampIncrementingCriteria criteria = null;
    if (schema.field(INCREMENTING_COLUMN.name()) != null) {
      if (schema.field(TS1_COLUMN.name()) != null) {
        criteria = criteriaIncTs;
      } else {
        criteria = criteriaInc;
      }
    } else if (schema.field(TS1_COLUMN.name()) != null) {
      criteria = criteriaTs;
    } else {
      criteria = this.criteria;
    }
    TimestampIncrementingOffset offset = criteria.extractValues(schema, record, null);
    assertEquals(expected, offset.getIncrementingOffset());
  }

  @Test
  public void extractIntOffset() throws SQLException {
    schema = SchemaBuilder.struct().field("id", SchemaBuilder.INT32_SCHEMA).build();
    record = new Struct(schema).put("id", 42);
    assertExtractedOffset(42L, schema, record);
  }

  @Test
  public void extractLongOffset() throws SQLException {
    schema = SchemaBuilder.struct().field("id", SchemaBuilder.INT64_SCHEMA).build();
    record = new Struct(schema).put("id", 42L);
    assertExtractedOffset(42L, schema, record);
  }

  @Test
  public void extractDecimalOffset() throws SQLException {
    final Schema decimalSchema = Decimal.schema(0);
    schema = SchemaBuilder.struct().field("id", decimalSchema).build();
    record = new Struct(schema).put("id", new BigDecimal(42));
    assertExtractedOffset(42L, schema, record);
  }

  @Test(expected = ConnectException.class)
  public void extractTooLargeDecimalOffset() throws SQLException {
    final Schema decimalSchema = Decimal.schema(0);
    schema = SchemaBuilder.struct().field("id", decimalSchema).build();
    record = new Struct(schema).put("id", new BigDecimal(Long.MAX_VALUE).add(new BigDecimal(1)));
    assertExtractedOffset(42L, schema, record);
  }

  @Test(expected = ConnectException.class)
  public void extractFractionalDecimalOffset() throws SQLException {
    final Schema decimalSchema = Decimal.schema(2);
    schema = SchemaBuilder.struct().field("id", decimalSchema).build();
    record = new Struct(schema).put("id", new BigDecimal("42.42"));
    assertExtractedOffset(42L, schema, record);
  }

  @Test
  public void extractWithIncColumn() throws SQLException {
    schema = SchemaBuilder.struct()
                          .field("id", SchemaBuilder.INT32_SCHEMA)
                          .field(TS1_COLUMN.name(), Timestamp.SCHEMA)
                          .field(TS2_COLUMN.name(), Timestamp.SCHEMA)
                          .build();
    record = new Struct(schema).put("id", 42);
    assertExtractedOffset(42L, schema, record);

  }
}