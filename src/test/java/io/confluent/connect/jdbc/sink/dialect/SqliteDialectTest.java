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

import static org.junit.Assert.assertEquals;

public class SqliteDialectTest extends BaseDialectTest {

  public SqliteDialectTest() {
    super(new SqliteDialect());
  }

  @Test
  public void dataTypeMappings() {
    verifyDataTypeMapping("INTEGER", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("REAL", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("REAL", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("INTEGER", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("TEXT", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("BLOB", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("NUMERIC", Decimal.schema(0));
    verifyDataTypeMapping("NUMERIC", Date.SCHEMA);
    verifyDataTypeMapping("NUMERIC", Time.SCHEMA);
    verifyDataTypeMapping("NUMERIC", Timestamp.SCHEMA);
  }

  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE `test` (" + System.lineSeparator() +
        "`col1` INTEGER NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE `test` (" + System.lineSeparator() +
        "`pk1` INTEGER NOT NULL," + System.lineSeparator() +
        "PRIMARY KEY(`pk1`))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE `test` (" + System.lineSeparator() +
        "`pk1` INTEGER NOT NULL," + System.lineSeparator() +
        "`pk2` INTEGER NOT NULL," + System.lineSeparator() +
        "`col1` INTEGER NOT NULL," + System.lineSeparator() +
        "PRIMARY KEY(`pk1`,`pk2`))"
    );
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol(
        "ALTER TABLE `test` ADD `newcol1` INTEGER NULL"
    );
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols(
        "ALTER TABLE `test` ADD `newcol1` INTEGER NULL",
        "ALTER TABLE `test` ADD `newcol2` INTEGER DEFAULT 42"
    );
  }

  @Test
  public void upsert() {
    assertEquals(
        "INSERT OR REPLACE INTO `Book`(`author`,`title`,`ISBN`,`year`,`pages`) VALUES(?,?,?,?,?)",
        dialect.getUpsertQuery("Book", Arrays.asList("author", "title"), Arrays.asList("ISBN", "year", "pages"))
    );
  }

}
