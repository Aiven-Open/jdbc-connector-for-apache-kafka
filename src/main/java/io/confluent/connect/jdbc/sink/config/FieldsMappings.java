package io.confluent.connect.jdbc.sink.config;

import io.confluent.connect.jdbc.sink.common.ParameterValidator;
import com.google.common.base.Joiner;

import java.util.Map;

/**
 * Contains the Schema field names to consider as well as their mappings to the target table columns
 */
public final class FieldsMappings {

  public final static String CONNECT_TOPIC_COLUMN = "__connect_topic";
  public final static String CONNECT_OFFSET_COLUMN = "__connect_offset";
  public final static String CONNECT_PARTITION_COLUMN = "__connect_partition";
  private final String tableName;
  private final String incomingTopic;
  private final boolean allFieldsIncluded;
  private final Map<String, FieldAlias> mappings;

  private final boolean autoCreateTable;
  private final boolean evolveTableSchema;
  private final boolean capitalizeColumnNamesAndTables;

  private final InsertModeEnum insertMode;

  /**
   * Creates a new instance of FieldsMappings
   *
   * @param tableName         - The target RDBMS table to insert the records into
   * @param incomingTopic     - The source Kafka topic
   * @param allFieldsIncluded - If set to true it considers all fields in the payload; if false it will rely on the
   *                          defined fields to include
   * @param insertMode        - Specifies how the data is inserted into the rdbms
   * @param mappings          - Provides the map of fields to include and their alias. It could be set to Map.empty if all fields
   *                          are to be included.
   * @param evolveTableSchema - If true it allows auto table creation and table evolution
   */
  public FieldsMappings(final String tableName,
                        final String incomingTopic,
                        final boolean allFieldsIncluded,
                        final InsertModeEnum insertMode,
                        final Map<String, FieldAlias> mappings,
                        final boolean autoCreateTable,
                        final boolean evolveTableSchema,
                        final boolean capitalizeColumnNamesAndTables) {
    this.capitalizeColumnNamesAndTables = capitalizeColumnNamesAndTables;

    ParameterValidator.notNullOrEmpty(tableName, "tableName");
    ParameterValidator.notNullOrEmpty(incomingTopic, "incomingTopic");
    ParameterValidator.notNull(mappings, "map");

    this.tableName = tableName;
    this.incomingTopic = incomingTopic;
    this.allFieldsIncluded = allFieldsIncluded;
    this.insertMode = insertMode;
    this.mappings = mappings;
    this.autoCreateTable = autoCreateTable;
    this.evolveTableSchema = evolveTableSchema;
  }


  /**
   * If set to true all the incoming SinkRecord payload fields are considered for inserting into the table.
   *
   * @return true - if all payload fields should be included; false - otherwise
   */
  public boolean areAllFieldsIncluded() {
    return allFieldsIncluded;
  }

  public Map<String, FieldAlias> getMappings() {
    return mappings;
  }

  /**
   * Returns the source Kafka topic.
   *
   * @return - The kafka topic name
   */
  public String getIncomingTopic() {
    return incomingTopic;
  }

  /**
   * Returns the target database table name.
   *
   * @return Table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Returns true if the table can be created; false - otherwise
   *
   * @return true if the table can be created; false - otherwise
   */
  public boolean autoCreateTable() {
    return autoCreateTable;
  }

  /**
   * Returns true if the table schema is suppose to be evolved in sync with Schema changes; false - otherwise
   *
   * @return true if the table schema is suppose to be evolved in sync with Schema changes; false - otherwise
   */
  public boolean evolveTableSchema() {
    return evolveTableSchema;
  }

  /**
   * Returns the way the data should be pushed into the database:insert/upsert
   *
   * @return - The insert mode
   */
  public InsertModeEnum getInsertMode() {
    return insertMode;
  }

  @Override
  public String toString() {
    Joiner.MapJoiner mapJoiner = Joiner.on(",\n").withKeyValueSeparator("=");
    return "{\n" +
            "topic:" + incomingTopic + "\n" +
            "table:" + tableName + "\n" +
            "auto-create:" + autoCreateTable + "\n" +
            "evolve-schema:" + evolveTableSchema + "\n" +
            "include-all-fields:" + allFieldsIncluded + "\n" +
            "mappings:" + mapJoiner.join(mappings) + "\n" +
            "}";
  }

  public boolean isCapitalizeColumnNamesAndTables() {
    return capitalizeColumnNamesAndTables;
  }
}
