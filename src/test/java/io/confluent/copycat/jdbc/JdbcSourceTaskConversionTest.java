/**
 * Copyright 2015 Confluent Inc.
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
 */

package io.confluent.copycat.jdbc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import javax.sql.rowset.serial.SerialBlob;

import io.confluent.copycat.data.GenericRecord;
import io.confluent.copycat.data.Schema;
import io.confluent.copycat.data.Schema.Field;
import io.confluent.copycat.data.Schema.Type;
import io.confluent.copycat.source.SourceRecord;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

// Tests conversion of data types and schemas. These use the types supported by Derby, which
// might not cover everything in the SQL standards and definitely doesn't cover any non-standard
// types, but should cover most of the JDBC types which is all we see anyway
public class JdbcSourceTaskConversionTest extends JdbcSourceTaskTestBase {

  @Before
  public void setup() throws Exception {
    super.setup();
    task.start(singleTableConfig());
  }

  @After
  public void tearDown() throws Exception {
    task.stop();
    super.tearDown();
  }

  @Test
  public void testBoolean() throws Exception {
    typeConversion("BOOLEAN", null, false, false, Type.BOOLEAN);
  }

  @Test
  public void testNullableBoolean() throws Exception {
    typeConversion("BOOLEAN", null, true, false, Type.BOOLEAN);
  }

  @Test
  public void testSmallInt() throws Exception {
    typeConversion("SMALLINT", null, false, 1, Type.INT);
  }

  @Test
  public void testNullableSmallInt() throws Exception {
    typeConversion("SMALLINT", null, true, 1, Type.INT);
  }

  @Test
  public void testInt() throws Exception {
    typeConversion("INTEGER", null, false, 1, Type.INT);
  }

  @Test
  public void testNullableInt() throws Exception {
    typeConversion("INTEGER", null, true, 1, Type.INT);
  }

  @Test
  public void testBigInt() throws Exception {
    typeConversion("BIGINT", null, false, Long.MAX_VALUE, Type.LONG);
  }

  @Test
  public void testNullableBigInt() throws Exception {
    typeConversion("BIGINT", null, true, Long.MAX_VALUE, Type.LONG);
  }

  @Test
  public void testReal() throws Exception {
    typeConversion("REAL", null, false, 1, Type.FLOAT);
  }

  @Test
  public void testNullableReal() throws Exception {
    typeConversion("REAL", null, true, 1, Type.FLOAT);
  }

  @Test
  public void testDouble() throws Exception {
    typeConversion("DOUBLE", null, false, 1, Type.DOUBLE);
  }

  @Test
  public void testNullableDouble() throws Exception {
    typeConversion("DOUBLE", null, true, 1, Type.DOUBLE);
  }

  @Test
  public void testChar() throws Exception {
    // Converted to string, so fixed size not checked
    typeConversion("CHAR(5)", null, false, "a", Type.STRING);
  }

  @Test
  public void testNullableChar() throws Exception {
    // Converted to string, so fixed size not checked
    typeConversion("CHAR(5)", null, true, "a", Type.STRING);
  }

  @Test
  public void testVarChar() throws Exception {
    // Converted to string, so fixed size not checked
    typeConversion("VARCHAR(5)", null, false, "a", Type.STRING);
  }

  @Test
  public void testNullableVarChar() throws Exception {
    // Converted to string, so fixed size not checked
    typeConversion("VARCHAR(5)", null, true, "a", Type.STRING);
  }

  @Test
  public void testBlob() throws Exception {
    // BLOB is varying size but can specify a max size so we specify that size in the spec but
    // expect BYTES not FIXED back.
    typeConversion("BLOB(5)", null, false, new SerialBlob("a".getBytes()), Type.BYTES);
  }

  @Test
  public void testNullableBlob() throws Exception {
    typeConversion("BLOB(5)", null, true, new SerialBlob("a".getBytes()), Type.BYTES);
  }

  @Test
  public void testClob() throws Exception {
    // CLOB is varying size but can specify a max size so we specify that size in the spec but
    // expect BYTES not FIXED back.
    typeConversion("CLOB(5)", null, false, "a", Type.STRING);
  }

  @Test
  public void testNullableClob() throws Exception {
    typeConversion("CLOB(5)", null, true, "a", Type.STRING);
  }

  @Test
  public void testBinary() throws Exception {
    typeConversion("CHAR(5) FOR BIT DATA", 5, false, "a".getBytes(), Type.FIXED);
  }

  @Test
  public void testNullableBinary() throws Exception {
    typeConversion("CHAR(5) FOR BIT DATA", 5, true, "a".getBytes(), Type.FIXED);
  }

  // FIXME DATE, TIME, and TIMESTAMP still need to be implemented in JdbcSourceTask
  // FIXME DECIMAL, NUMERIC still need to be implemented in JdbcSourceTask

  // Derby has an XML type, but the JDBC driver doesn't implement any of the type bindings,
  // returning strings instead, so the XML type is not tested here

  private void typeConversion(String sqlType, Integer fixedSize, boolean nullable,
                              Object sqlValue, Type convertedType) throws Exception {
    String sqlColumnSpec = sqlType;
    if (!nullable) {
      sqlColumnSpec += " NOT NULL";
    }
    db.createTable(SINGLE_TABLE_NAME, "id", sqlColumnSpec);
    db.insert(SINGLE_TABLE_NAME, "id", sqlValue);
    List<SourceRecord> records = task.poll();
    validateRecords(records, convertedType, nullable, fixedSize);
  }

  /**
   * Validates schema and type of returned record data. Assumes single-field values since this is
   * only used for validating type information.
   */
  private void validateRecords(List<SourceRecord> records, Type type,
                               boolean nullable, Integer fixedSize) {
    // Validate # of records and object type
    assertEquals(1, records.size());
    Object objValue = records.get(0).getValue();
    assertTrue(objValue instanceof GenericRecord);
    GenericRecord value = (GenericRecord) objValue;

    // Validate schema
    Schema schema = value.getSchema();
    assertEquals(Type.RECORD, schema.getType());
    List<Field> fields = schema.getFields();
    assertEquals(1, fields.size());

    Schema fieldSchema = fields.get(0).schema();
    if (nullable) {
      assertEquals(Type.UNION, fieldSchema.getType());
      List<Schema> unionSchemas = fieldSchema.getTypes();
      assertEquals(2, unionSchemas.size());
      Schema nullSchema = unionSchemas.get(0);
      assertEquals(Type.NULL, nullSchema.getType());
      // Now treat the non-null schema as if it were the schema in a non-nullable schema
      fieldSchema = unionSchemas.get(1);
    }

    assertEquals(type, fieldSchema.getType());
    if (fixedSize != null) {
      assertEquals((long) fixedSize, (long) fieldSchema.getFixedSize());
    } else if (type == Type.FIXED) {
      fail("Types that generate a fixed field should provide the size of the fixed byte[] to "
           + "validate");
    }
  }
}
