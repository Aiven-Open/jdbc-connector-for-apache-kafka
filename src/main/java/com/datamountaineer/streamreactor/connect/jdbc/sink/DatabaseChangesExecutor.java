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
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect.DbDialect;
import com.google.common.base.Joiner;
import io.confluent.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Controls the database changes - creating amending tables
 */
public class DatabaseChangesExecutor {
  private final static Logger logger = LoggerFactory.getLogger(DatabaseChangesExecutor.class);

  private final Set<String> tablesAllowingAutoCreate;
  private final Set<String> tablesAllowingSchemaEvolution;
  private final DatabaseMetadata databaseMetadata;
  private final DbDialect dbDialect;

  public DatabaseChangesExecutor(final Set<String> tablesAllowingAutoCreate,
                                 final Set<String> tablesAllowingSchemaEvolution,
                                 final DatabaseMetadata databaseMetadata,
                                 final DbDialect dbDialect) {
    ParameterValidator.notNull(databaseMetadata, "databaseMetadata");
    ParameterValidator.notNull(tablesAllowingAutoCreate, "tablesAllowingAutoCreate");
    ParameterValidator.notNull(tablesAllowingSchemaEvolution, "tablesAllowingSchemaEvolution");
    ParameterValidator.notNull(dbDialect, "dbDialect");

    this.tablesAllowingAutoCreate = tablesAllowingAutoCreate;
    this.tablesAllowingSchemaEvolution = tablesAllowingSchemaEvolution;
    this.databaseMetadata = databaseMetadata;
    this.dbDialect = dbDialect;
  }


  public void handleChanges(final Map<String, Collection<Field>> tablesToColumnsMap,
                            final Connection connection) throws SQLException {
    DatabaseMetadata.Changes changes = databaseMetadata.getChanges(tablesToColumnsMap);

    final List<String> queries = new ArrayList<>();
    handleNewTables(changes.getCreatedMap(), queries);
    handleAmendTables(changes.getAmendmentMap(), queries);
    if (queries.size() > 0) {
      final String query = Joiner.on(String.format(";%s", System.lineSeparator())).join(queries);
      logger.info(String.format("Changing database structure for database %s%s%s",
              databaseMetadata.getDatabaseName(),
              System.lineSeparator(),
              query));

      Statement statement = null;
      try {
        statement = connection.createStatement();
        statement.execute(query);
      } finally {
        try {
          if (statement != null) {
            statement.close();
          }
        } catch (SQLException e) {
          logger.error(e.getMessage(), e);
        }
      }
    }
  }

  private void handleNewTables(final Map<String, Collection<Field>> createdMap,
                               final List<String> queries) {
    if (createdMap == null) {
      return;
    }
    for (final Map.Entry<String, Collection<Field>> entry : createdMap.entrySet()) {
      final String tableName = entry.getKey();
      if (databaseMetadata.containsTable(tableName)) {
        continue;
      }
      if (!tablesAllowingAutoCreate.contains(tableName)) {
        throw new ConfigException(String.format("Table %s is not configured with auto-create", entry.getKey()));
      }
      final String createTable = dbDialect.getCreateQuery(tableName, entry.getValue());
      queries.add(createTable);
    }
  }

  private void handleAmendTables(final Map<String, Collection<Field>> createdMap,
                                 final List<String> queries) {
    if (createdMap == null) {
      return;
    }
    for (final Map.Entry<String, Collection<Field>> entry : createdMap.entrySet()) {
      final String tableName = entry.getKey();
      if (!databaseMetadata.containsTable(tableName)) {
        throw new RuntimeException(String.format("%s is set for amendments but hasn't been created yet", entry.getKey()));
      }
      if (!tablesAllowingSchemaEvolution.contains(entry.getKey())) {
        logger.warn(String.format("Table %s is not configured with schema evolution", entry.getKey()));
        continue;
      }
      final String createTable = dbDialect.getAlterTable(entry.getKey(), entry.getValue());
      queries.add(createTable);
    }
  }
}
