package com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect;


import com.datamountaineer.streamreactor.connect.jdbc.sink.Field;
import com.google.common.collect.Lists;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class SqliteDialectTest {
  @Test
  public void validateAlterTable() {
    List<String> queries = new SQLiteDialect().getAlterTable("tableA", Lists.newArrayList(
            new Field(Schema.Type.BOOLEAN, "col1", false),
            new Field(Schema.Type.FLOAT32, "col2", false),
            new Field(Schema.Type.STRING, "col3", false)
    ));

    assertEquals(3, queries.size());
    assertEquals("ALTER TABLE tableA ADD `col1` NUMERIC NULL;", queries.get(0));
    assertEquals("ALTER TABLE tableA ADD `col2` REAL NULL;", queries.get(1));
    assertEquals("ALTER TABLE tableA ADD `col3` TEXT NULL;", queries.get(2));
  }
}
