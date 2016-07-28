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

import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;

import static org.junit.Assert.assertEquals;

public class SqlServerDialectTest {
  private final DbDialect dialect = new SqlServerDialect();

  @Test
  public void produceTheRightSqlStatementWhithASinglePK() {
    String insert = dialect.getUpsertQuery("Customer", Collections.singletonList("id"), Arrays.asList("name", "salary", "address"));
    assertEquals(insert, "merge into [Customer] with (HOLDLOCK) AS target using (select ? AS [name], ? AS [salary], ? AS " +
                         "[address], ? AS [id]) AS incoming on (target.[id]=incoming.[id]) when matched then update set " +
                         "[name]=incoming.[name],[salary]=incoming.[salary],[address]=incoming.[address] when not matched then insert " +
                         "([name], [salary], [address], [id]) values (incoming.[name],incoming.[salary],incoming.[address],incoming.[id]);");

  }

  @Test
  public void produceTheRightSqlStatementWhithACompositePK() {
    String insert = dialect.getUpsertQuery("Book", Arrays.asList("author", "title"), Arrays.asList("ISBN", "year", "pages"));
    assertEquals(insert, "merge into [Book] with (HOLDLOCK) AS target using (select ? AS [ISBN], ? AS [year], ? AS [pages], " +
                         "? AS [author], ? AS [title]) AS incoming on (target.[author]=incoming.[author] and target.[title]=incoming.[title])" +
                         " when matched then update set [ISBN]=incoming.[ISBN],[year]=incoming.[year],[pages]=incoming.[pages] when not " +
                         "matched then insert ([ISBN], [year], [pages], [author], [title]) values (incoming.[ISBN],incoming.[year]," +
                         "incoming.[pages],incoming.[author],incoming.[title]);");

  }


  @Test
  public void handleCreateTableMultiplePKColumns() {
    String actual = dialect.getCreateQuery("tableA", Arrays.asList(
        new SinkRecordField(Schema.Type.INT32, "userid", true),
        new SinkRecordField(Schema.Type.INT32, "userdataid", true),
        new SinkRecordField(Schema.Type.STRING, "info", false)
    ));

    String expected = "CREATE TABLE [tableA] (" + System.lineSeparator() +
                      "[userid] int NOT NULL," + System.lineSeparator() +
                      "[userdataid] int NOT NULL," + System.lineSeparator() +
                      "[info] varchar(max) NULL," + System.lineSeparator() +
                      "PRIMARY KEY([userid],[userdataid]))";
    assertEquals(expected, actual);
  }

  @Test
  public void handleCreateTableOnePKColumn() {
    String actual = dialect.getCreateQuery("tableA", Arrays.asList(
        new SinkRecordField(Schema.Type.INT32, "col1", true),
        new SinkRecordField(Schema.Type.INT64, "col2", false),
        new SinkRecordField(Schema.Type.STRING, "col3", false),
        new SinkRecordField(Schema.Type.FLOAT32, "col4", false),
        new SinkRecordField(Schema.Type.FLOAT64, "col5", false),
        new SinkRecordField(Schema.Type.BOOLEAN, "col6", false),
        new SinkRecordField(Schema.Type.INT8, "col7", false),
        new SinkRecordField(Schema.Type.INT16, "col8", false)
    ));

    String expected = "CREATE TABLE [tableA] (" + System.lineSeparator() +
                      "[col1] int NOT NULL," + System.lineSeparator() +
                      "[col2] bigint NULL," + System.lineSeparator() +
                      "[col3] varchar(max) NULL," + System.lineSeparator() +
                      "[col4] real NULL," + System.lineSeparator() +
                      "[col5] float NULL," + System.lineSeparator() +
                      "[col6] bit NULL," + System.lineSeparator() +
                      "[col7] tinyint NULL," + System.lineSeparator() +
                      "[col8] smallint NULL," + System.lineSeparator() +
                      "PRIMARY KEY([col1]))";
    assertEquals(expected, actual);
  }

  @Test
  public void handleCreateTableNoPKColumn() {
    String actual = dialect.getCreateQuery("tableA", Arrays.asList(
        new SinkRecordField(Schema.Type.INT32, "col1", false),
        new SinkRecordField(Schema.Type.INT64, "col2", false),
        new SinkRecordField(Schema.Type.STRING, "col3", false),
        new SinkRecordField(Schema.Type.FLOAT32, "col4", false),
        new SinkRecordField(Schema.Type.FLOAT64, "col5", false),
        new SinkRecordField(Schema.Type.BOOLEAN, "col6", false),
        new SinkRecordField(Schema.Type.INT8, "col7", false),
        new SinkRecordField(Schema.Type.INT16, "col8", false)
    ));

    String expected = "CREATE TABLE [tableA] (" + System.lineSeparator() +
                      "[col1] int NULL," + System.lineSeparator() +
                      "[col2] bigint NULL," + System.lineSeparator() +
                      "[col3] varchar(max) NULL," + System.lineSeparator() +
                      "[col4] real NULL," + System.lineSeparator() +
                      "[col5] float NULL," + System.lineSeparator() +
                      "[col6] bit NULL," + System.lineSeparator() +
                      "[col7] tinyint NULL," + System.lineSeparator() +
                      "[col8] smallint NULL)";
    assertEquals(expected, actual);
  }

  @Test
  public void handleAmendAddColumns() {
    List<String> actual = dialect.getAlterTable("tableA", Arrays.asList(
        new SinkRecordField(Schema.Type.INT32, "col1", false),
        new SinkRecordField(Schema.Type.INT64, "col2", false),
        new SinkRecordField(Schema.Type.STRING, "col3", false),
        new SinkRecordField(Schema.Type.FLOAT32, "col4", false),
        new SinkRecordField(Schema.Type.FLOAT64, "col5", false),
        new SinkRecordField(Schema.Type.BOOLEAN, "col6", false),
        new SinkRecordField(Schema.Type.INT8, "col7", false),
        new SinkRecordField(Schema.Type.INT16, "col8", false)
    ));

    assertEquals(1, actual.size());

    String expected = "ALTER TABLE [tableA] ADD" + System.lineSeparator() +
                      "[col1] int NULL," + System.lineSeparator() +
                      "[col2] bigint NULL," + System.lineSeparator() +
                      "[col3] varchar(max) NULL," + System.lineSeparator() +
                      "[col4] real NULL," + System.lineSeparator() +
                      "[col5] float NULL," + System.lineSeparator() +
                      "[col6] bit NULL," + System.lineSeparator() +
                      "[col7] tinyint NULL," + System.lineSeparator() +
                      "[col8] smallint NULL";
    assertEquals(expected, actual.get(0));
  }
}
