package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.jdbc.common.DatabaseMetadata;
import com.datamountaineer.streamreactor.connect.jdbc.common.DbTable;
import com.datamountaineer.streamreactor.connect.jdbc.common.DbTableColumn;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldAlias;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldsMappings;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.InsertModeEnum;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkSettings;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.PreparedStatementBuilderHelper;
import com.google.common.collect.Lists;
import org.apache.kafka.common.config.*;
import org.junit.Test;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import java.util.ArrayList;

import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.DATABASE_CONNECTION_URI;
import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.TOPIC_TABLE_MAPPING;

public class PreparedStatementBuilderHelperTest {

  @Test(expected = ConfigException.class)
  public void shouldThrowAnExceptionIfTheMappedTableDoesntExistInTheDatabase() {
    Map<String, String> props = new HashMap<>();

    props.put(DATABASE_CONNECTION_URI, "jdbc://");
    props.put(TOPIC_TABLE_MAPPING, "topic1=tableA,topic2=tableNotPresent");

    JdbcSinkConfig config = new JdbcSinkConfig(props);
    JdbcSinkSettings settings = JdbcSinkSettings.from(config);

    List<DbTable> tables = Lists.newArrayList(
        new DbTable("tableA", new ArrayList<DbTableColumn>()),
        new DbTable("tableB", new ArrayList<DbTableColumn>()));
    PreparedStatementBuilderHelper.from(settings, new DatabaseMetadata(null, tables));
  }

  @Test(expected = ConfigException.class)
  public void throwAnExceptionIfTheFieldMapsToAnInExistingColumn() {

    List<DbTableColumn> columns = Lists.newArrayList(
        new DbTableColumn("col1", false, false, 1),
        new DbTableColumn("col2", false, false, 1),
        new DbTableColumn("col3", false, false, 1));
    DbTable table = new DbTable("tableA", columns);

    Map<String, FieldAlias> aliasMap = new HashMap<>();
    aliasMap.put("f1", new FieldAlias("col1", false));
    aliasMap.put("f2", new FieldAlias("col2", false));
    aliasMap.put("f3", new FieldAlias("colNotPresent", false));

    FieldsMappings mappings = new FieldsMappings("tableA", "topic1", false, aliasMap);
    PreparedStatementBuilderHelper.validateAndMerge(mappings, table, InsertModeEnum.INSERT);
  }


  @Test
  public void handleAllFieldsMappingSetting() {
    List<DbTableColumn> columns = Lists.newArrayList(
        new DbTableColumn("col1", true, false, 1),
        new DbTableColumn("col2", false, false, 1),
        new DbTableColumn("col3", false, false, 1));
    DbTable table = new DbTable("tableA", columns);

    Map<String, FieldAlias> aliasMap = new HashMap<>();

    FieldsMappings mappings = new FieldsMappings("tableA", "topic1", true, aliasMap);

    FieldsMappings newMappings = PreparedStatementBuilderHelper.validateAndMerge(mappings, table, InsertModeEnum.INSERT);
    assertEquals(newMappings.getTableName(), mappings.getTableName());
    assertEquals(newMappings.getIncomingTopic(), mappings.getIncomingTopic());
    assertEquals(newMappings.areAllFieldsIncluded(), false);

    Map<String, FieldAlias> newAliasMap = newMappings.getMappings();
    assertEquals(3, newAliasMap.size());
    assertTrue(newAliasMap.containsKey("col1"));
    assertEquals(newAliasMap.get("col1").getName(), "col1");
    assertEquals(newAliasMap.get("col1").isPrimaryKey(), true);

    assertTrue(newAliasMap.containsKey("col2"));
    assertEquals(newAliasMap.get("col2").getName(), "col2");
    assertEquals(newAliasMap.get("col2").isPrimaryKey(), false);

    assertTrue(newAliasMap.containsKey("col3"));
    assertEquals(newAliasMap.get("col3").getName(), "col3");
    assertEquals(newAliasMap.get("col3").isPrimaryKey(), false);
  }

  @Test
  public void handleAllFieldsMappingSettingAndTheMappingsProvided() {
    List<DbTableColumn> columns = Lists.newArrayList(
        new DbTableColumn("col1", true, false, 1),
        new DbTableColumn("col2", false, false, 1),
        new DbTableColumn("col3", false, false, 1));
    DbTable table = new DbTable("tableA", columns);

    Map<String, FieldAlias> aliasMap = new HashMap<>();
    aliasMap.put("f1", new FieldAlias("col3"));
    FieldsMappings mappings = new FieldsMappings("tableA", "topic1", true, aliasMap);

    FieldsMappings newMappings = PreparedStatementBuilderHelper.validateAndMerge(mappings, table, InsertModeEnum.INSERT);
    assertEquals(newMappings.getTableName(), mappings.getTableName());
    assertEquals(newMappings.getIncomingTopic(), mappings.getIncomingTopic());
    assertEquals(newMappings.areAllFieldsIncluded(), false);

    Map<String, FieldAlias> newAliasMap = newMappings.getMappings();
    assertEquals(4, newAliasMap.size()); //+ the specific mapping
    assertTrue(newAliasMap.containsKey("col1"));
    assertEquals(newAliasMap.get("col1").getName(), "col1");
    assertEquals(newAliasMap.get("col1").isPrimaryKey(), true);

    assertTrue(newAliasMap.containsKey("col2"));
    assertEquals(newAliasMap.get("col2").getName(), "col2");
    assertEquals(newAliasMap.get("col2").isPrimaryKey(), false);

    assertTrue(newAliasMap.containsKey("col3"));
    assertEquals(newAliasMap.get("col3").getName(), "col3");
    assertEquals(newAliasMap.get("col3").isPrimaryKey(), false);

    assertTrue(newAliasMap.containsKey("f1"));
    assertEquals(newAliasMap.get("f1").getName(), "col3");
    assertEquals(newAliasMap.get("f1").isPrimaryKey(), false);
  }


  @Test
  public void handleAllFieldsIncludedAndAnExistingMapping() {
    List<DbTableColumn> columns = Lists.newArrayList(
        new DbTableColumn("col1", true, false, 1),
        new DbTableColumn("col2", false, false, 1),
        new DbTableColumn("col3", false, false, 1));
    DbTable table = new DbTable("tableA", columns);

    Map<String, FieldAlias> aliasMap = new HashMap<>();
    aliasMap.put("col3", new FieldAlias("col3", true));
    FieldsMappings mappings = new FieldsMappings("tableA", "topic1", true, aliasMap);

    FieldsMappings newMappings = PreparedStatementBuilderHelper.validateAndMerge(mappings, table, InsertModeEnum.INSERT);
    assertEquals(newMappings.getTableName(), mappings.getTableName());
    assertEquals(newMappings.getIncomingTopic(), mappings.getIncomingTopic());
    assertEquals(newMappings.areAllFieldsIncluded(), false);

    Map<String, FieldAlias> newAliasMap = newMappings.getMappings();
    assertEquals(3, newAliasMap.size()); //+ the specific mapping
    assertTrue(newAliasMap.containsKey("col1"));
    assertEquals(newAliasMap.get("col1").getName(), "col1");
    assertEquals(newAliasMap.get("col1").isPrimaryKey(), true);

    assertTrue(newAliasMap.containsKey("col2"));
    assertEquals(newAliasMap.get("col2").getName(), "col2");
    assertEquals(newAliasMap.get("col2").isPrimaryKey(), false);

    assertTrue(newAliasMap.containsKey("col3"));
    assertEquals(newAliasMap.get("col3").getName(), "col3");
    assertEquals(newAliasMap.get("col3").isPrimaryKey(), false);
  }


  @Test
  public void handleNotAllFieldsMappedButSpecificMappings() {
    List<DbTableColumn> columns = Lists.newArrayList(
        new DbTableColumn("col1", true, false, 1),
        new DbTableColumn("col2", false, false, 1),
        new DbTableColumn("col3", false, false, 1));
    DbTable table = new DbTable("tableA", columns);

    Map<String, FieldAlias> aliasMap = new HashMap<>();
    aliasMap.put("col1", new FieldAlias("col1", false));
    aliasMap.put("col3", new FieldAlias("col3", false));
    FieldsMappings mappings = new FieldsMappings("tableA", "topic1", false, aliasMap);

    FieldsMappings newMappings = PreparedStatementBuilderHelper.validateAndMerge(mappings, table, InsertModeEnum.INSERT);
    assertEquals(newMappings.getTableName(), mappings.getTableName());
    assertEquals(newMappings.getIncomingTopic(), mappings.getIncomingTopic());
    assertEquals(newMappings.areAllFieldsIncluded(), false);

    Map<String, FieldAlias> newAliasMap = newMappings.getMappings();
    assertEquals(2, newAliasMap.size()); //+ the specific mapping
    assertTrue(newAliasMap.containsKey("col1"));
    assertEquals(newAliasMap.get("col1").getName(), "col1");
    assertEquals(newAliasMap.get("col1").isPrimaryKey(), true);

    assertTrue(newAliasMap.containsKey("col3"));
    assertEquals(newAliasMap.get("col3").getName(), "col3");
    assertEquals(newAliasMap.get("col3").isPrimaryKey(), false);
  }

  @Test(expected = ConfigException.class)
  public void throwAnExceptionIfSpecificMappingsAreSetButPKColumnIsMissed() {
    List<DbTableColumn> columns = Lists.newArrayList(
        new DbTableColumn("col1", true, false, 1),
        new DbTableColumn("col2", false, false, 1),
        new DbTableColumn("col3", false, false, 1));
    DbTable table = new DbTable("tableA", columns);

    Map<String, FieldAlias> aliasMap = new HashMap<>();
    aliasMap.put("col2", new FieldAlias("col2", false));
    aliasMap.put("col3", new FieldAlias("col3", false));
    FieldsMappings mappings = new FieldsMappings("tableA", "topic1", false, aliasMap);

    PreparedStatementBuilderHelper.validateAndMerge(mappings, table, InsertModeEnum.UPSERT);
  }
}