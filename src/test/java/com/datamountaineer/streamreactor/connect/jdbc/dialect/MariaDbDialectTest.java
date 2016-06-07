package com.datamountaineer.streamreactor.connect.jdbc.dialect;

import com.datamountaineer.streamreactor.connect.jdbc.sink.SinkRecordField;
import com.google.common.collect.Lists;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class MariaDbDialectTest {
  private final MariaDialect dialect = new MariaDialect();

  @Test
  public void handleCreateTableMultiplePKColumns() {
    String actual = dialect.getCreateQuery("tableA", Lists.newArrayList(
            new SinkRecordField(Schema.Type.INT32, "userid", true),
            new SinkRecordField(Schema.Type.INT32, "userdataid", true),
            new SinkRecordField(Schema.Type.STRING, "info", false)
    ));

    String expected = "CREATE TABLE `tableA` (" + System.lineSeparator() +
            "`userid` INTEGER NOT NULL," + System.lineSeparator() +
            "`userdataid` INTEGER NOT NULL," + System.lineSeparator() +
            "`info` VARCHAR(256) NULL," + System.lineSeparator() +
            "PRIMARY KEY(`userid`,`userdataid`))";
    assertEquals(expected, actual);
  }

  @Test
  public void handleCreateTableOnePKColumn() {
    String actual = dialect.getCreateQuery("tableA", Lists.newArrayList(
            new SinkRecordField(Schema.Type.INT32, "col1", true),
            new SinkRecordField(Schema.Type.INT64, "col2", false),
            new SinkRecordField(Schema.Type.STRING, "col3", false),
            new SinkRecordField(Schema.Type.FLOAT32, "col4", false),
            new SinkRecordField(Schema.Type.FLOAT64, "col5", false),
            new SinkRecordField(Schema.Type.BOOLEAN, "col6", false),
            new SinkRecordField(Schema.Type.INT8, "col7", false),
            new SinkRecordField(Schema.Type.INT16, "col8", false)
    ));

    String expected = "CREATE TABLE `tableA` (" + System.lineSeparator() +
            "`col1` INTEGER NOT NULL," + System.lineSeparator() +
            "`col2` BIGINT NULL," + System.lineSeparator() +
            "`col3` VARCHAR(256) NULL," + System.lineSeparator() +
            "`col4` FLOAT NULL," + System.lineSeparator() +
            "`col5` DOUBLE NULL," + System.lineSeparator() +
            "`col6` TINYINT NULL," + System.lineSeparator() +
            "`col7` TINYINT NULL," + System.lineSeparator() +
            "`col8` SMALLINT NULL," + System.lineSeparator() +
            "PRIMARY KEY(`col1`))";
    assertEquals(expected, actual);
  }

  @Test
  public void handleCreateTableNoPKColumn() {
    String actual = dialect.getCreateQuery("tableA", Lists.newArrayList(
            new SinkRecordField(Schema.Type.INT32, "col1", false),
            new SinkRecordField(Schema.Type.INT64, "col2", false),
            new SinkRecordField(Schema.Type.STRING, "col3", false),
            new SinkRecordField(Schema.Type.FLOAT32, "col4", false),
            new SinkRecordField(Schema.Type.FLOAT64, "col5", false),
            new SinkRecordField(Schema.Type.BOOLEAN, "col6", false),
            new SinkRecordField(Schema.Type.INT8, "col7", false),
            new SinkRecordField(Schema.Type.INT16, "col8", false)
    ));

    String expected = "CREATE TABLE `tableA` (" + System.lineSeparator() +
            "`col1` INTEGER NULL," + System.lineSeparator() +
            "`col2` BIGINT NULL," + System.lineSeparator() +
            "`col3` VARCHAR(256) NULL," + System.lineSeparator() +
            "`col4` FLOAT NULL," + System.lineSeparator() +
            "`col5` DOUBLE NULL," + System.lineSeparator() +
            "`col6` TINYINT NULL," + System.lineSeparator() +
            "`col7` TINYINT NULL," + System.lineSeparator() +
            "`col8` SMALLINT NULL)";
    assertEquals(expected, actual);
  }

  @Test
  public void handleAmendAddColumns() {
    List<String> actual = dialect.getAlterTable("tableA", Lists.newArrayList(
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

    String expected = "ALTER TABLE `tableA` " + System.lineSeparator() +
            "ADD COLUMN `col1` INTEGER NULL," + System.lineSeparator() +
            "ADD COLUMN `col2` BIGINT NULL," + System.lineSeparator() +
            "ADD COLUMN `col3` VARCHAR(256) NULL," + System.lineSeparator() +
            "ADD COLUMN `col4` FLOAT NULL," + System.lineSeparator() +
            "ADD COLUMN `col5` DOUBLE NULL," + System.lineSeparator() +
            "ADD COLUMN `col6` TINYINT NULL," + System.lineSeparator() +
            "ADD COLUMN `col7` TINYINT NULL," + System.lineSeparator() +
            "ADD COLUMN `col8` SMALLINT NULL";
    assertEquals(expected, actual.get(0));
  }

  @Test
  public void createTheUpsertQuery() {
    String expected = "insert into `actor`(`first_name`,`last_name`,`score`,`actor_id`) " +
            "values(?,?,?,?) on duplicate key update `first_name`=values(`first_name`),`last_name`=values(`last_name`)," +
            "`score`=values(`score`)";

    String upsert = dialect.getUpsertQuery("actor",
            Lists.newArrayList("first_name", "last_name", "score"),
            Lists.newArrayList("actor_id"));
    assertEquals(expected, upsert);
  }
}
