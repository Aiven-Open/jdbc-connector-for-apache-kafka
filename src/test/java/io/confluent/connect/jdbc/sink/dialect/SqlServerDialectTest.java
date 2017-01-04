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

public class SqlServerDialectTest extends BaseDialectTest {

  public SqlServerDialectTest() {
    super(new SqlServerDialect());
  }

  @Test
  public void dataTypeMappings() {
    verifyDataTypeMapping("tinyint", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("smallint", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("int", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("bigint", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("real", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("float", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("bit", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("varchar(max)", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("varbinary(max)", Schema.BYTES_SCHEMA);
    verifyDataTypeMapping("decimal(38,0)", Decimal.schema(0));
    verifyDataTypeMapping("decimal(38,4)", Decimal.schema(4));
    verifyDataTypeMapping("date", Date.SCHEMA);
    verifyDataTypeMapping("time", Time.SCHEMA);
    verifyDataTypeMapping("datetime2", Timestamp.SCHEMA);
  }

  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE [test] (" + System.lineSeparator() +
        "[col1] int NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE [test] (" + System.lineSeparator() +
        "[pk1] int NOT NULL," + System.lineSeparator() +
        "PRIMARY KEY([pk1]))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE [test] (" + System.lineSeparator() +
        "[pk1] int NOT NULL," + System.lineSeparator() +
        "[pk2] int NOT NULL," + System.lineSeparator() +
        "[col1] int NOT NULL," + System.lineSeparator() +
        "PRIMARY KEY([pk1],[pk2]))"
    );
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol(
        "ALTER TABLE [test] ADD" + System.lineSeparator()
        + "[newcol1] int NULL"
    );
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols(
        "ALTER TABLE [test] ADD" + System.lineSeparator()
        + "[newcol1] int NULL," + System.lineSeparator()
        + "[newcol2] int DEFAULT 42"
    );
  }

  @Test
  public void upsert1() {
    assertEquals(
        "merge into [Customer] with (HOLDLOCK) AS target using (select ? AS [id], ? AS [name], ? AS [salary], ? AS [address]) "
        + "AS incoming on (target.[id]=incoming.[id]) when matched then update set "
        + "[name]=incoming.[name],[salary]=incoming.[salary],[address]=incoming.[address] when not matched then insert "
        + "([name], [salary], [address], [id]) values (incoming.[name],incoming.[salary],incoming.[address],incoming.[id]);",
        dialect.getUpsertQuery("Customer", Collections.singletonList("id"), Arrays.asList("name", "salary", "address"))
    );
  }

  @Test
  public void upsert2() {
    assertEquals(
        "merge into [Book] with (HOLDLOCK) AS target using (select ? AS [author], ? AS [title], ? AS [ISBN], ? AS [year], ? AS [pages])"
        + " AS incoming on (target.[author]=incoming.[author] and target.[title]=incoming.[title])"
        + " when matched then update set [ISBN]=incoming.[ISBN],[year]=incoming.[year],[pages]=incoming.[pages] when not "
        + "matched then insert ([ISBN], [year], [pages], [author], [title]) values (incoming.[ISBN],incoming.[year],"
        + "incoming.[pages],incoming.[author],incoming.[title]);",
        dialect.getUpsertQuery("Book", Arrays.asList("author", "title"), Arrays.asList("ISBN", "year", "pages"))
    );
  }

}
