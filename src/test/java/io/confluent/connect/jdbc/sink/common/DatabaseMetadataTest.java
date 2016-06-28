package io.confluent.connect.jdbc.sink.common;


import io.confluent.connect.jdbc.sink.SinkRecordField;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DatabaseMetadataTest {
  @Test
  public void shouldNotRaiseAnExceptionIfTableNameIsNull() {
    new DatabaseMetadata(null, new ArrayList<DbTable>());
  }

  @Test(expected = IllegalArgumentException.class)
  public void raiseAnExceptionIfTableNameIsWhitespace() {
    new DatabaseMetadata("  ", new ArrayList<DbTable>());
  }

  @Test(expected = IllegalArgumentException.class)
  public void raiseAnExceptionIfTheTablesListIsNull() {
    new DatabaseMetadata("somedatabasename", null);
  }

  @Test
  public void createAnInstance() {
    String database = "somedatabase";
    List<DbTable> tables = Lists.newArrayList(
            new DbTable("table1",
                    Lists.newArrayList(
                            new DbTableColumn("col1", true, false, 1),
                            new DbTableColumn("col2", false, false, 1)
                    )),
            new DbTable("table2",
                    Lists.newArrayList(
                            new DbTableColumn("col1", false, false, 1),
                            new DbTableColumn("col2", false, false, 1),
                            new DbTableColumn("col3", false, false, 1)
                    )));
    DatabaseMetadata dbMetadata = new DatabaseMetadata(database, tables);

    assertEquals(dbMetadata.getDatabaseName(), database);
    assertNull(dbMetadata.getTable("nonexisting"));
    assertEquals(dbMetadata.getTable("table1").getName(), "table1");
    assertEquals(dbMetadata.getTable("table1").getColumns().size(), tables.get(0).getColumns().size());

    assertEquals(dbMetadata.getTable("table1").getColumns().get("col1").getSqlType(),
            tables.get(0).getColumns().get("col1").getSqlType());
    assertEquals(dbMetadata.getTable("table1").getColumns().get("col1").getName(),
            tables.get(0).getColumns().get("col1").getName());
    assertEquals(dbMetadata.getTable("table1").getColumns().get("col1").isPrimaryKey(),
            tables.get(0).getColumns().get("col1").isPrimaryKey());

    assertEquals(dbMetadata.getTable("table2").getColumns().get("col3").getSqlType(),
            tables.get(1).getColumns().get("col3").getSqlType());
    assertEquals(dbMetadata.getTable("table2").getColumns().get("col3").getName(),
            tables.get(1).getColumns().get("col3").getName());
    assertEquals(dbMetadata.getTable("table2").getColumns().get("col3").isPrimaryKey(),
            tables.get(1).getColumns().get("col3").isPrimaryKey());
  }

  @Test
  public void returnsNoAmendmentsAndNoNewDataForEmptyMapInput() {
    String database = "somedatabase";
    List<DbTable> tables = Lists.newArrayList(
            new DbTable("table1",
                    Lists.newArrayList(
                            new DbTableColumn("col1", true, false, 1),
                            new DbTableColumn("col2", false, false, 1)
                    )),
            new DbTable("table2",
                    Lists.newArrayList(
                            new DbTableColumn("col1", false, false, 1),
                            new DbTableColumn("col2", false, false, 1),
                            new DbTableColumn("col3", false, false, 1)
                    )));
    DatabaseMetadata dbMetadata = new DatabaseMetadata(database, tables);

    DatabaseMetadata.Changes result = dbMetadata.getChanges(new HashMap<String, Collection<SinkRecordField>>());
    assertNull(result.getCreatedMap());
    assertNull(result.getAmendmentMap());
  }

  @Test(expected = IllegalArgumentException.class)
  public void returnsNoAmendmentsAndNoNewDataForNullMapInput() {
    String database = "somedatabase";
    List<DbTable> tables = Lists.newArrayList(
            new DbTable("table1",
                    Lists.newArrayList(
                            new DbTableColumn("col1", true, false, 1),
                            new DbTableColumn("col2", false, false, 1)
                    )),
            new DbTable("table2",
                    Lists.newArrayList(
                            new DbTableColumn("col1", false, false, 1),
                            new DbTableColumn("col2", false, false, 1),
                            new DbTableColumn("col3", false, false, 1)
                    )));
    DatabaseMetadata dbMetadata = new DatabaseMetadata(database, tables);

    DatabaseMetadata.Changes result = dbMetadata.getChanges(null);
    assertNull(result.getCreatedMap());
    assertNull(result.getAmendmentMap());
  }

  @Test
  public void handleAmendments() {
    String database = "somedatabase";
    List<DbTable> tables = Lists.newArrayList(
            new DbTable("table1",
                    Lists.newArrayList(
                            new DbTableColumn("col1", true, false, 1),
                            new DbTableColumn("col2", false, false, 1)
                    )),
            new DbTable("table2",
                    Lists.newArrayList(
                            new DbTableColumn("col1", false, false, 1),
                            new DbTableColumn("col2", false, false, 1),
                            new DbTableColumn("col3", false, false, 1)
                    )));
    DatabaseMetadata dbMetadata = new DatabaseMetadata(database, tables);

    Map<String, Collection<SinkRecordField>> map = new HashMap<>();
    map.put("table1",
            Lists.newArrayList(
                    new SinkRecordField(Schema.Type.INT32, "col1", true),
                    new SinkRecordField(Schema.Type.STRING, "col3", false),
                    new SinkRecordField(Schema.Type.STRING, "col2", false),
                    new SinkRecordField(Schema.Type.STRING, "col4", false)
            ));

    map.put("table2",
            Lists.newArrayList(
                    new SinkRecordField(Schema.Type.INT32, "col1", true),
                    new SinkRecordField(Schema.Type.STRING, "col2", false),
                    new SinkRecordField(Schema.Type.STRING, "col3", false)
            ));
    DatabaseMetadata.Changes diff = dbMetadata.getChanges(map);
    assertNull(diff.getCreatedMap());
    Map<String, Collection<SinkRecordField>> amendmentsMap = diff.getAmendmentMap();
    assertEquals(amendmentsMap.size(), 1);
    assertTrue(amendmentsMap.containsKey("table1"));
    Collection<SinkRecordField> fields = amendmentsMap.get("table1");
    assertEquals(fields.size(), 2);
    Set<String> fieldSet = Sets.newHashSet(
            Iterables.transform(fields, new Function<SinkRecordField, String>() {
              @Override
              public String apply(SinkRecordField field) {
                return field.getName();
              }
            }));

    assertEquals(fieldSet.size(), 2);
    assertTrue(fieldSet.contains("col3"));
    assertTrue(fieldSet.contains("col4"));
  }

  @Test
  public void handleNewTableAddition() {
    String database = "somedatabase";
    List<DbTable> tables = Lists.newArrayList(
            new DbTable("table1",
                    Lists.newArrayList(
                            new DbTableColumn("col1", true, false, 1),
                            new DbTableColumn("col2", false, false, 1)
                    )),
            new DbTable("table2",
                    Lists.newArrayList(
                            new DbTableColumn("col1", false, false, 1),
                            new DbTableColumn("col2", false, false, 1),
                            new DbTableColumn("col3", false, false, 1)
                    )));
    DatabaseMetadata dbMetadata = new DatabaseMetadata(database, tables);

    Map<String, Collection<SinkRecordField>> map = new HashMap<>();
    map.put("table1a",
            Lists.newArrayList(
                    new SinkRecordField(Schema.Type.INT32, "col1", true),
                    new SinkRecordField(Schema.Type.STRING, "col3", true),
                    new SinkRecordField(Schema.Type.STRING, "col2", false)
            ));

    map.put("table2",
            Lists.newArrayList(
                    new SinkRecordField(Schema.Type.INT32, "col1", true),
                    new SinkRecordField(Schema.Type.STRING, "col2", false),
                    new SinkRecordField(Schema.Type.STRING, "col3", false)
            ));
    DatabaseMetadata.Changes diff = dbMetadata.getChanges(map);
    assertNull(diff.getAmendmentMap());
    Map<String, Collection<SinkRecordField>> createdMap = diff.getCreatedMap();
    assertEquals(createdMap.size(), 1);
    assertTrue(createdMap.containsKey("table1a"));
    Collection<SinkRecordField> fields = createdMap.get("table1a");
    assertEquals(fields.size(), 3);
    Set<String> fieldSet = Sets.newHashSet(
            Iterables.transform(fields, new Function<SinkRecordField, String>() {
              @Override
              public String apply(SinkRecordField field) {
                return field.getName();
              }
            }));

    assertEquals(fieldSet.size(), 3);
    assertTrue(fieldSet.contains("col1"));
    assertTrue(fieldSet.contains("col2"));
    assertTrue(fieldSet.contains("col3"));
  }
}
