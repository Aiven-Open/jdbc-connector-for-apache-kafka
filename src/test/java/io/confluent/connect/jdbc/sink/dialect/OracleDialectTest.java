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

public class OracleDialectTest extends BaseDialectTest {

  public OracleDialectTest() {
    super(new OracleDialect());
  }

  @Test
  public void dataTypeMappings() {
    verifyDataTypeMapping("NUMBER(3,0)", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("NUMBER(5,0)", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("NUMBER(10,0)", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("NUMBER(19,0)", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("BINARY_FLOAT", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("BINARY_DOUBLE", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("NUMBER(1,0)", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("CLOB", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("BLOB", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("NUMBER(*,0)", Decimal.schema(0));
    verifyDataTypeMapping("NUMBER(*,42)", Decimal.schema(42));
    verifyDataTypeMapping("DATE", Date.SCHEMA);
    verifyDataTypeMapping("DATE", Time.SCHEMA);
    verifyDataTypeMapping("TIMESTAMP", Timestamp.SCHEMA);
  }

  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE \"test\" (" + System.lineSeparator() +
        "\"col1\" NUMBER(10,0) NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE \"test\" (" + System.lineSeparator() +
        "\"pk1\" NUMBER(10,0) NOT NULL," + System.lineSeparator() +
        "PRIMARY KEY(\"pk1\"))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE \"test\" (" + System.lineSeparator() +
        "\"pk1\" NUMBER(10,0) NOT NULL," + System.lineSeparator() +
        "\"pk2\" NUMBER(10,0) NOT NULL," + System.lineSeparator() +
        "\"col1\" NUMBER(10,0) NOT NULL," + System.lineSeparator() +
        "PRIMARY KEY(\"pk1\",\"pk2\"))"
    );
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol(
        "ALTER TABLE \"test\" ADD(" + System.lineSeparator()
        + "\"newcol1\" NUMBER(10,0) NULL)"
    );
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols(
        "ALTER TABLE \"test\" ADD(" + System.lineSeparator()
        + "\"newcol1\" NUMBER(10,0) NULL," + System.lineSeparator()
        + "\"newcol2\" NUMBER(10,0) DEFAULT 42)"
    );
  }

  @Test
  public void upsert() {
    assertEquals(
        "merge into \"ARTICLE\" " +
        "using (select ? \"title\", ? \"author\", ? \"body\" FROM dual) incoming on" +
        "(\"ARTICLE\".\"title\"=incoming.\"title\" and \"ARTICLE\".\"author\"=incoming.\"author\") " +
        "when matched then update set \"ARTICLE\".\"body\"=incoming.\"body\" " +
        "when not matched then insert(\"ARTICLE\".\"body\",\"ARTICLE\".\"title\",\"ARTICLE\".\"author\") " +
        "values(incoming.\"body\",incoming.\"title\",incoming.\"author\")",
        dialect.getUpsertQuery("ARTICLE", Arrays.asList("title", "author"), Collections.singletonList("body"))
    );
  }

}