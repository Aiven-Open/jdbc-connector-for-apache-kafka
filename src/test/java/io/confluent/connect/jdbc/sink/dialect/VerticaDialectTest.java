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

public class VerticaDialectTest extends BaseDialectTest {

  public VerticaDialectTest() {
    super(new VerticaDialect());
  }

  @Test
  public void dataTypeMappings() {
    verifyDataTypeMapping("INT", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("INT", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("INT", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("INT", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("FLOAT", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("FLOAT", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("BOOLEAN", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("VARCHAR(1024)", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("VARBINARY(1024)", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("DECIMAL(18,0)", Decimal.schema(0));
    verifyDataTypeMapping("DECIMAL(18,4)", Decimal.schema(4));
    verifyDataTypeMapping("DATE", Date.SCHEMA);
    verifyDataTypeMapping("TIME", Time.SCHEMA);
    verifyDataTypeMapping("TIMESTAMP", Timestamp.SCHEMA);
  }

  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE \"test\" (" + System.lineSeparator() +
        "\"col1\" INT NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE \"test\" (" + System.lineSeparator() +
        "\"pk1\" INT NOT NULL," + System.lineSeparator() +
        "PRIMARY KEY(\"pk1\"))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE \"test\" (" + System.lineSeparator() +
        "\"pk1\" INT NOT NULL," + System.lineSeparator() +
        "\"pk2\" INT NOT NULL," + System.lineSeparator() +
        "\"col1\" INT NOT NULL," + System.lineSeparator() +
        "PRIMARY KEY(\"pk1\",\"pk2\"))"
    );
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol(
        "ALTER TABLE \"test\" ADD \"newcol1\" INT NULL"
    );
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols(
        "ALTER TABLE \"test\" ADD \"newcol1\" INT NULL",
        "ALTER TABLE \"test\" ADD \"newcol2\" INT DEFAULT 42"
    );
  }
}
