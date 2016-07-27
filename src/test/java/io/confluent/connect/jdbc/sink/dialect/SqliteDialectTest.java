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
import java.util.List;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;

import static org.junit.Assert.assertEquals;

public class SqliteDialectTest {
  @Test
  public void validateAlterTable() {
    List<String> queries = new SqliteDialect().getAlterTable("tableA", Arrays.asList(
        new SinkRecordField(Schema.Type.BOOLEAN, "col1", false),
        new SinkRecordField(Schema.Type.FLOAT32, "col2", false),
        new SinkRecordField(Schema.Type.STRING, "col3", false)
    ));

    assertEquals(3, queries.size());
    assertEquals("ALTER TABLE `tableA` ADD `col1` NUMERIC NULL", queries.get(0));
    assertEquals("ALTER TABLE `tableA` ADD `col2` REAL NULL", queries.get(1));
    assertEquals("ALTER TABLE `tableA` ADD `col3` TEXT NULL", queries.get(2));
  }

  @Test
  public void produceTheRightSqlStatementWhithACompositePK() {
    String insert = new SqliteDialect().getUpsertQuery("Book", Arrays.asList("author", "title"), Arrays.asList("ISBN", "year", "pages"));
    assertEquals("INSERT OR REPLACE INTO `Book`(`author`,`title`,`ISBN`,`year`,`pages`) VALUES(?,?,?,?,?)", insert);

  }
}
