package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldAlias;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldsMappings;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkSettings;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.PreparedStatementBuilderHelper;
import com.google.common.collect.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.config.*;
import org.apache.kafka.connect.sink.*;
import org.junit.Test;
import org.mockito.*;

import java.util.*;

import static com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkConfig.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class PreparedStatementBuilderHelperTest {

  @Test(expected = ConfigException.class)
  public void shouldThrowAnExceptionIfTheMappedTableDoesntExistInTheDatabase() {
    Map<String, String> props = new HashMap<String, String>();
    String topic1 = "topic1";
    String topic2 = "topic2";
    TopicPartition tp1 = new TopicPartition(topic1, 12);
    TopicPartition tp2 = new TopicPartition(topic2, 13);
    HashSet<TopicPartition> assignment = Sets.newHashSet();

    //Set topic assignments, used by the sinkContext mock
    assignment.add(tp1);
    assignment.add(tp2);

    //mock the context
    SinkTaskContext context = Mockito.mock(SinkTaskContext.class);
    when(context.assignment()).thenReturn(assignment);

    props.put(DATABASE_CONNECTION_URI, "jdbc://");
    props.put(JAR_FILE, "jdbc.jar");
    props.put(DRIVER_MANAGER_CLASS, "OracleDriver");

    JdbcSinkConfig config = new JdbcSinkConfig(props);
        //JdbcSinkSettings.fixConfigLimitationOnDynamicProps(props);
    JdbcSinkSettings settings = JdbcSinkSettings.from(config);

    Map<String, DbTable> tableMap = new HashMap<>();
    tableMap.put("tableA", new DbTable("tableA", new HashMap<String, DbTableColumn>()));
    tableMap.put("tableB", new DbTable("tableB", new HashMap<String, DbTableColumn>()));
    PreparedStatementBuilderHelper.from(settings, tableMap);
  }

  @Test(expected = ConfigException.class)
  public void throwAnExceptionIfTheFieldMapsToAnInexistentColumn() {


    Map<String, DbTableColumn> columnMap = new HashMap<>();
    columnMap.put("col1", new DbTableColumn("col1", false, false, 1));
    columnMap.put("col2", new DbTableColumn("col2", false, false, 1));
    columnMap.put("col3", new DbTableColumn("col3", false, false, 1));
    DbTable table = new DbTable("tableA", columnMap);

    Map<String, FieldAlias> aliasMap = new HashMap<>();
    aliasMap.put("f1", new FieldAlias("col1", false));
    aliasMap.put("f2", new FieldAlias("col2", false));
    aliasMap.put("f3", new FieldAlias("colNotPresent", false));

    FieldsMappings mappings = new FieldsMappings("tableA", "topic1", false, aliasMap);
    PreparedStatementBuilderHelper.validateAndMerge(mappings, table);
  }


  @Test
  public void handleAllFieldsMappingSetting() {
    Map<String, DbTableColumn> columnMap = new HashMap<>();
    columnMap.put("col1", new DbTableColumn("col1", true, false, 1));
    columnMap.put("col2", new DbTableColumn("col2", false, false, 1));
    columnMap.put("col3", new DbTableColumn("col3", false, false, 1));
    DbTable table = new DbTable("tableA", columnMap);

    Map<String, FieldAlias> aliasMap = new HashMap<>();

    FieldsMappings mappings = new FieldsMappings("tableA", "topic1", true, aliasMap);

    FieldsMappings newMappings = PreparedStatementBuilderHelper.validateAndMerge(mappings, table);
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
    Map<String, DbTableColumn> columnMap = new HashMap<>();
    columnMap.put("col1", new DbTableColumn("col1", true, false, 1));
    columnMap.put("col2", new DbTableColumn("col2", false, false, 1));
    columnMap.put("col3", new DbTableColumn("col3", false, false, 1));
    DbTable table = new DbTable("tableA", columnMap);

    Map<String, FieldAlias> aliasMap = new HashMap<>();
    aliasMap.put("f1", new FieldAlias("col3"));
    FieldsMappings mappings = new FieldsMappings("tableA", "topic1", true, aliasMap);

    FieldsMappings newMappings = PreparedStatementBuilderHelper.validateAndMerge(mappings, table);
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
    Map<String, DbTableColumn> columnMap = new HashMap<>();
    columnMap.put("col1", new DbTableColumn("col1", true, false, 1));
    columnMap.put("col2", new DbTableColumn("col2", false, false, 1));
    columnMap.put("col3", new DbTableColumn("col3", false, false, 1));
    DbTable table = new DbTable("tableA", columnMap);

    Map<String, FieldAlias> aliasMap = new HashMap<>();
    aliasMap.put("col3", new FieldAlias("col3", true));
    FieldsMappings mappings = new FieldsMappings("tableA", "topic1", true, aliasMap);

    FieldsMappings newMappings = PreparedStatementBuilderHelper.validateAndMerge(mappings, table);
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
    Map<String, DbTableColumn> columnMap = new HashMap<>();
    columnMap.put("col1", new DbTableColumn("col1", true, false, 1));
    columnMap.put("col2", new DbTableColumn("col2", false, false, 1));
    columnMap.put("col3", new DbTableColumn("col3", false, false, 1));
    DbTable table = new DbTable("tableA", columnMap);

    Map<String, FieldAlias> aliasMap = new HashMap<>();
    aliasMap.put("col1", new FieldAlias("col1", false));
    aliasMap.put("col3", new FieldAlias("col3", false));
    FieldsMappings mappings = new FieldsMappings("tableA", "topic1", false, aliasMap);

    FieldsMappings newMappings = PreparedStatementBuilderHelper.validateAndMerge(mappings, table);
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
    Map<String, DbTableColumn> columnMap = new HashMap<>();
    columnMap.put("col1", new DbTableColumn("col1", true, false, 1));
    columnMap.put("col2", new DbTableColumn("col2", false, false, 1));
    columnMap.put("col3", new DbTableColumn("col3", false, false, 1));
    DbTable table = new DbTable("tableA", columnMap);

    Map<String, FieldAlias> aliasMap = new HashMap<>();
    aliasMap.put("col2", new FieldAlias("col2", false));
    aliasMap.put("col3", new FieldAlias("col3", false));
    FieldsMappings mappings = new FieldsMappings("tableA", "topic1", false, aliasMap);

    PreparedStatementBuilderHelper.validateAndMerge(mappings, table);
  }
}
