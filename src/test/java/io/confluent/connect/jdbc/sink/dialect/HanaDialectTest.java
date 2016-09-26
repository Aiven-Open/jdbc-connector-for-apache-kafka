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

package io.confluent.connect.jdbc.sink.dialect;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class HanaDialectTest extends BaseDialectTest {

  public HanaDialectTest() {
    super(new HanaDialect());
  }

  @Test
  public void dataTypeMappings() {
    verifyDataTypeMapping("TINYINT", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("SMALLINT", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("BIGINT", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("REAL", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("DOUBLE", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("BOOLEAN", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("VARCHAR(1000)", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("BLOB", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("DECIMAL", Decimal.schema(0));
    verifyDataTypeMapping("DATE", Date.SCHEMA);
    verifyDataTypeMapping("DATE", Time.SCHEMA);
    verifyDataTypeMapping("TIMESTAMP", Timestamp.SCHEMA);
  }

  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE COLUMN TABLE \"test\" (" + System.lineSeparator() +
        "\"col1\" INTEGER NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE COLUMN TABLE \"test\" (" + System.lineSeparator() +
        "\"pk1\" INTEGER NOT NULL," + System.lineSeparator() +
        "PRIMARY KEY(\"pk1\"))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE COLUMN TABLE \"test\" (" + System.lineSeparator() +
        "\"pk1\" INTEGER NOT NULL," + System.lineSeparator() +
        "\"pk2\" INTEGER NOT NULL," + System.lineSeparator() +
        "\"col1\" INTEGER NOT NULL," + System.lineSeparator() +
        "PRIMARY KEY(\"pk1\",\"pk2\"))"
    );
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol(
        "ALTER TABLE \"test\" ADD(" + System.lineSeparator()
        + "\"newcol1\" INTEGER NULL)"
    );
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols(
        "ALTER TABLE \"test\" ADD(" + System.lineSeparator()
        + "\"newcol1\" INTEGER NULL," + System.lineSeparator()
        + "\"newcol2\" INTEGER DEFAULT 42)"
    );
  }

  @Test
  public void upsert() {
    assertEquals(
        "UPSERT \"tableA\"(\"col1\",\"col2\",\"col3\",\"col4\") VALUES(?,?,?,?) WITH PRIMARY KEY",
        dialect.getUpsertQuery("tableA", Collections.singletonList("col1"), Arrays.asList("col2", "col3", "col4"))
    );
  }

}
