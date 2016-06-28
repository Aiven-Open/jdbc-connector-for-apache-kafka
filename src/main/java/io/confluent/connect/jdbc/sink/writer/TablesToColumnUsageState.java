package io.confluent.connect.jdbc.sink.writer;

import io.confluent.connect.jdbc.sink.common.ParameterValidator;
import io.confluent.connect.jdbc.sink.SinkRecordField;
import io.confluent.connect.jdbc.sink.binders.PreparedStatementBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Used by the PreparedStatements to track which tables are used and which columns
 */
final class TablesToColumnUsageState {
  private final static Logger logger = LoggerFactory.getLogger(TablesToColumnUsageState.class);
  private final Map<String, Map<String, SinkRecordField>> tablesToColumnsMap = new HashMap<>();

  /**
   * Returns the state a list of database tables and the columns targeted.
   *
   * @return The state a list of databases
   */
  public Map<String, Collection<SinkRecordField>> getState() {
    Map<String, Collection<SinkRecordField>> state = new HashMap<>();
    for (final Map.Entry<String, Map<String, SinkRecordField>> entry : tablesToColumnsMap.entrySet()) {
      final Collection<SinkRecordField> fields = entry.getValue().values();
      state.put(entry.getKey(), fields);
    }
    return state;
  }

  /**
   * Updates is local state from the given parameters.
   *
   * @param table   - The database table to get the new data
   * @param binders - A collection of PreparedStatementBinders containing the field/column and the schema type
   */
  public void trackUsage(final String table, final List<PreparedStatementBinder> binders) {
    ParameterValidator.notNullOrEmpty(table, "table");
    ParameterValidator.notNull(binders, "binders");
    if (binders.isEmpty()) {
      return;
    }
    Map<String, SinkRecordField> fieldMap;
    if (!tablesToColumnsMap.containsKey(table)) {
      fieldMap = new HashMap<>();
      tablesToColumnsMap.put(table, fieldMap);
    } else {
      fieldMap = tablesToColumnsMap.get(table);
    }

    addFields(fieldMap, binders);
  }

  /**
   * Adds a new record to the target if that field name is not present already
   *
   * @param target  - A map of fields/columns already seen
   * @param binders - A collection of PreparedStatementBinder each one containing the field/column and the the schema type
   */
  private static void addFields(final Map<String, SinkRecordField> target,
                                final Collection<PreparedStatementBinder> binders) {
    if (binders == null) {
      return;
    }
    for (final PreparedStatementBinder binder : binders) {
      if (!target.containsKey(binder.getFieldName())) {
        logger.debug("adding new field to " + binder.getFieldName());
        target.put(binder.getFieldName(), new SinkRecordField(binder.getFieldType(), binder.getFieldName(), binder.isPrimaryKey()));
      }
    }
  }
}
