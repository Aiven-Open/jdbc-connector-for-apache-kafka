/**
 * Copyright 2015 Datamountaineer.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/


package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;

import com.datamountaineer.streamreactor.connect.jdbc.sink.Field;
import com.datamountaineer.streamreactor.connect.jdbc.sink.StructFieldsDataExtractor;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.PreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.common.ParameterValidator;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Used by the PreparedStatements to track which tables are used and which columns
 */
final class TablesToColumnUsageState {
  private final Map<String, Map<String, Field>> tablesToColumnsMap = new HashMap<>();

  /**
   * Returns the state a list of database tables and the columns targeted.
   *
   * @return The state a list of databases
   */
  public Map<String, Collection<Field>> getState() {
    Map<String, Collection<Field>> state = new HashMap<>();
    for (final Map.Entry<String, Map<String, Field>> entry : tablesToColumnsMap.entrySet()) {
      final Collection<Field> fields = entry.getValue().values();
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
  public void trackUsage(final String table, StructFieldsDataExtractor.PreparedStatementBinders binders) {
    ParameterValidator.notNullOrEmpty(table, "table");
    ParameterValidator.notNull(binders, "binders");
    if (binders.isEmpty()) {
      return;
    }
    Map<String, Field> fieldMap;
    if (!tablesToColumnsMap.containsKey(table)) {
      fieldMap = new HashMap<>();
      tablesToColumnsMap.put(table, fieldMap);
    } else {
      fieldMap = tablesToColumnsMap.get(table);
    }

    addFields(fieldMap, binders.getKeyColumns(), true);
    addFields(fieldMap, binders.getNonKeyColumns(), false);
  }

  /**
   * Adds a new record to the target if that field name is not present already
   *
   * @param target  - A map of fields/columns already seen
   * @param binders - A collection of PreparedStatementBinder each one containing the field/column and the the schema type
   * @param primaryKeys       If true all the binders are for primary key columns;
   */
  private static void addFields(final Map<String, Field> target,
                                final Collection<PreparedStatementBinder> binders,
                                final boolean primaryKeys) {
    if (binders == null) {
      return;
    }
    for (final PreparedStatementBinder binder : binders) {
      if (!target.containsKey(binder.getFieldName())) {
        target.put(binder.getFieldName(), new Field(binder.getFieldType(), binder.getFieldName(), primaryKeys));
      }
    }
  }
}
