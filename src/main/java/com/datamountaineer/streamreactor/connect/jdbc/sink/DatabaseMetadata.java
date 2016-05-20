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

package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.jdbc.sink.common.ParameterValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Contains all the database tables metadata.
 */
public class DatabaseMetadata {
  private static final Logger logger = LoggerFactory.getLogger(DatabaseMetadata.class);
  private final String databaseName;
  private final Map<String, DbTable> tables;

  public DatabaseMetadata(String databaseName, List<DbTable> tables) {
    //we support null because SqLite does return as database
    if (databaseName != null && databaseName.trim().length() == 0) {
      throw new IllegalArgumentException("<databasename> is not valid.");
    }
    ParameterValidator.notNull(tables, "tables");
    this.databaseName = databaseName;
    this.tables = new HashMap<>();
    for (final DbTable t : tables) {
      this.tables.put(t.getName(), t);
    }
  }

  public DbTable getTable(final String tableName) {
    return tables.get(tableName);
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public Changes getChanges(final Map<String, Collection<Field>> tableColumnsMap) {
    ParameterValidator.notNull(tableColumnsMap, "tableColumnsMap");
    Map<String, Collection<Field>> created = null;
    Map<String, Collection<Field>> amended = null;
    for (final Map.Entry<String, Collection<Field>> entry : tableColumnsMap.entrySet()) {
      if (!tables.containsKey(entry.getKey())) {
        //we don't have this table
        if (created == null) created = new HashMap<>();
        created.put(entry.getKey(), tableColumnsMap.get(entry.getKey()));
      } else {
        final DbTable table = tables.get(entry.getKey());
        final Map<String, DbTableColumn> existingColumnsMap = table.getColumns();
        for (final Field field : entry.getValue()) {
          if (!existingColumnsMap.containsKey(field.getName())) {
            if (amended == null) {
              amended = new HashMap<>();
            }
            //new field which hasn't been seen before
            if (!amended.containsKey(table.getName())) {
              amended.put(table.getName(), new ArrayList<Field>());
            }

            final Collection<Field> newFileds = amended.get(table.getName());
            newFileds.add(field);
          }
        }
      }
    }
    return new Changes(amended, created);
  }

  public boolean containsTable(final String tableName) {
    return tables.containsKey(tableName);
  }

  public Collection<String> getTableNames() {
    return tables.keySet();
  }

  public final class Changes {
    private final Map<String, Collection<Field>> amendmentMap;
    private final Map<String, Collection<Field>> createdMap;

    public Changes(Map<String, Collection<Field>> amendmentMap, Map<String, Collection<Field>> createdMap) {
      this.amendmentMap = amendmentMap;
      this.createdMap = createdMap;
    }

    public Map<String, Collection<Field>> getAmendmentMap() {
      return amendmentMap;
    }

    public Map<String, Collection<Field>> getCreatedMap() {
      return createdMap;
    }
  }
}