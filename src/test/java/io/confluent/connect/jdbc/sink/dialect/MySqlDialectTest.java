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

public class MySqlDialectTest extends BaseDialectTest {

  public MySqlDialectTest() {
    super(new MySqlDialect());
  }

  @Test
  public void dataTypeMappings() {
    verifyDataTypeMapping("TINYINT", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("SMALLINT", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("INT", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("BIGINT", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("FLOAT", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("DOUBLE", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("TINYINT", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("VARCHAR(256)", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("VARBINARY(1024)", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("DECIMAL(65,0)", Decimal.schema(0));
    verifyDataTypeMapping("DECIMAL(65,2)", Decimal.schema(2));
    verifyDataTypeMapping("DATE", Date.SCHEMA);
    verifyDataTypeMapping("TIME(3)", Time.SCHEMA);
    verifyDataTypeMapping("DATETIME(3)", Timestamp.SCHEMA);
  }

  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE `test` (" + System.lineSeparator() +
        "`col1` INT NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE `test` (" + System.lineSeparator() +
        "`pk1` INT NOT NULL," + System.lineSeparator() +
        "PRIMARY KEY(`pk1`))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE `test` (" + System.lineSeparator() +
        "`pk1` INT NOT NULL," + System.lineSeparator() +
        "`pk2` INT NOT NULL," + System.lineSeparator() +
        "`col1` INT NOT NULL," + System.lineSeparator() +
        "PRIMARY KEY(`pk1`,`pk2`))"
    );
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol(
        "ALTER TABLE `test` ADD `newcol1` INT NULL"
    );
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols(
        "ALTER TABLE `test` " + System.lineSeparator()
        + "ADD `newcol1` INT NULL," + System.lineSeparator()
        + "ADD `newcol2` INT DEFAULT 42"
    );
  }

  @Test
  public void upsert() {
    assertEquals(
        "insert into `actor`(`actor_id`,`first_name`,`last_name`,`score`) " +
        "values(?,?,?,?) on duplicate key update `first_name`=values(`first_name`),`last_name`=values(`last_name`),`score`=values(`score`)",
        dialect.getUpsertQuery("actor", Arrays.asList("actor_id"), Arrays.asList("first_name", "last_name", "score"))
    );
  }

  @Test
  public void upsertOnlyKeyCols() {
    assertEquals(
        "insert into `actor`(`actor_id`) " +
        "values(?) on duplicate key update `actor_id`=values(`actor_id`)",
        dialect.getUpsertQuery("actor", Arrays.asList("actor_id"), Collections.<String>emptyList())
    );
  }

  @Test
  public void insert() {
    assertEquals(
        "INSERT INTO `customers`(`age`,`firstName`,`lastName`) VALUES(?,?,?)",
        dialect.getInsert("customers", Collections.<String>emptyList(), Arrays.asList("age", "firstName", "lastName"))
    );
  }

}
