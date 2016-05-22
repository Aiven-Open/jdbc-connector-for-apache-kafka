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

import com.datamountaineer.streamreactor.connect.jdbc.sink.DatabaseMetadata;
import com.datamountaineer.streamreactor.connect.jdbc.sink.DbTable;
import com.datamountaineer.streamreactor.connect.jdbc.sink.DbTableColumn;
import com.datamountaineer.streamreactor.connect.jdbc.sink.StructFieldsDataExtractor;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldAlias;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldsMappings;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkSettings;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class PreparedStatementBuilderHelper {

  /**
   * Creates a new instance of PrepareStatementBuilder
   *
   * @param settings - Instance of the Jdbc sink settings
   * @return - Returns an instance of PreparedStatementBuilder depending on the settings asking for batched or
   * non-batched inserts
   */
  public static PreparedStatementBuilder from(final JdbcSinkSettings settings) {
    final Map<String, StructFieldsDataExtractor> map = Maps.newHashMap();
    for (final FieldsMappings tm : settings.getMappings()) {
      final StructFieldsDataExtractor fieldsValuesExtractor = new StructFieldsDataExtractor(tm);

      map.put(tm.getIncomingTopic().toLowerCase(), fieldsValuesExtractor);
    }

    final QueryBuilder queryBuilder = QueryBuilderHelper.from(settings);

    if (settings.isBatching()) {
      return new BatchedPreparedStatementBuilder(map, queryBuilder);
    }

    return new SinglePreparedStatementBuilder(map, queryBuilder);
  }

  /**
   * This is used in the non evolving table mode. It will overlap the database columns information over the one provided
   * by the user - provides validation and also allows for columns auto mapping (Say user flags all fields to be included
   * and the payload has only 3 ouf of the 4 incoming mapping. It will therefore only consider the 3 fields discarding the
   * 4th avoiding the error)
   *
   * @param settings
   * @param databaseMetadata
   * @return
   */
  public static PreparedStatementBuilder from(final JdbcSinkSettings settings,
                                              DatabaseMetadata databaseMetadata) {

    final Map<String, StructFieldsDataExtractor> map = Maps.newHashMap();
    for (final FieldsMappings tm : settings.getMappings()) {
      if (!databaseMetadata.containsTable(tm.getTableName())) {
        final String tables = Joiner.on(",").join(databaseMetadata.getTableNames());
        throw new ConfigException(String.format("%s table is not found in the database available tables:%s",
                tm.getTableName(),
                tables));
      }

      FieldsMappings tableMappings = tm;
      if (!tm.autoCreateTable()) {
        tableMappings = validateAndMerge(tm, databaseMetadata.getTable(tm.getTableName()));
      }

      final StructFieldsDataExtractor fieldsValuesExtractor = new StructFieldsDataExtractor(tableMappings);

      map.put(tm.getIncomingTopic().toLowerCase(), fieldsValuesExtractor);
    }

    final QueryBuilder queryBuilder = QueryBuilderHelper.from(settings);

    if (settings.isBatching()) {
      return new BatchedPreparedStatementBuilder(map, queryBuilder);
    }

    return new SinglePreparedStatementBuilder(map, queryBuilder);
  }

  /**
   * In non table evolution we merge the field mappings with the database structure. This way we avoid the problems
   * caused when the user sets * for all fields the payload has 4 fields but only 3 found in the database.
   * Furthermore if the mapping is not found in the database an exception is thrown
   *
   * @param tm      - Field mappings
   * @param dbTable - The instance of DbTable
   * @return
   */
  public static FieldsMappings validateAndMerge(final FieldsMappings tm, final DbTable dbTable) {
    final Set<String> pkColumns = new HashSet<>();
    final Map<String, DbTableColumn> dbCols = dbTable.getColumns();
    for (DbTableColumn column : dbCols.values()) {
      if (column.isPrimaryKey()) {
        pkColumns.add(column.getName());
      }
    }
    final Map<String, FieldAlias> map = new HashMap<>();
    if (tm.areAllFieldsIncluded()) {

      for (DbTableColumn column : dbTable.getColumns().values()) {
        map.put(column.getName(),
                new FieldAlias(column.getName(), column.isPrimaryKey()));
      }

      //apply the specific mappings
      for (Map.Entry<String, FieldAlias> alias : tm.getMappings().entrySet()) {
        final String colName = alias.getValue().getName();
        if (!map.containsKey(colName)) {
          final String error =
                  String.format("Invalid field mapping. For table %s the following column is not found %s in available columns:%s",
                          tm.getTableName(),
                          colName,
                          Joiner.on(",").join(map.keySet()));
          throw new ConfigException(error);
        }
        map.put(alias.getKey(), new FieldAlias(colName, pkColumns.contains(colName)));
      }
    } else {

      final Set<String> specifiedPKs = new HashSet<>();
      //in this case just validate the mappings
      for (Map.Entry<String, FieldAlias> alias : tm.getMappings().entrySet()) {
        final String colName = alias.getValue().getName();
        if (!dbCols.containsKey(colName)) {
          final String error =
                  String.format("Invalid field mapping. For table %s the following column is not found %s in available columns:%s",
                          tm.getTableName(),
                          colName,
                          Joiner.on(",").join(dbCols.keySet()));
          throw new ConfigException(error);
        }
        map.put(alias.getKey(), new FieldAlias(colName, pkColumns.contains(colName)));
        if (pkColumns.contains(colName)) {
          specifiedPKs.add(colName);
        }
      }

      if (pkColumns.size() > 0) {
        if (!specifiedPKs.containsAll(pkColumns)) {
          throw new ConfigException(
                  String.format("Invalid mappings. Not all PK columns have been specified. PK specified %s  out of existing %s",
                          Joiner.on(",").join(specifiedPKs),
                          Joiner.on(",").join(pkColumns)));
        }
      }
    }
    return new FieldsMappings(tm.getTableName(), tm.getIncomingTopic(), false, map);
  }
}