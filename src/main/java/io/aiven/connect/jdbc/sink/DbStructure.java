/*
 * Copyright 2020 Aiven Oy
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.connect.jdbc.sink;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.sink.metadata.FieldsMetadata;
import io.aiven.connect.jdbc.sink.metadata.SinkRecordField;
import io.aiven.connect.jdbc.util.TableDefinition;
import io.aiven.connect.jdbc.util.TableDefinitions;
import io.aiven.connect.jdbc.util.TableId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbStructure {
    private static final Logger log = LoggerFactory.getLogger(DbStructure.class);

    private final DatabaseDialect dbDialect;
    private final TableDefinitions tableDefns;

    public DbStructure(final DatabaseDialect dbDialect) {
        this.dbDialect = dbDialect;
        this.tableDefns = new TableDefinitions(dbDialect);
    }

    public TableDefinition tableDefinitionFor(final TableId tableId, final Connection connection) throws SQLException {
        final var tblDefinition = tableDefns.get(connection, tableId);
        if (Objects.nonNull(tblDefinition)) {
            return tblDefinition;
        } else {
            return tableDefns.refresh(connection, tableId);
        }
    }

    /**
     * @return whether a DDL operation was performed
     * @throws SQLException if a DDL operation was deemed necessary but failed
     */
    public boolean createOrAmendIfNecessary(
            final JdbcSinkConfig config,
            final Connection connection,
            final TableId tableId,
            final FieldsMetadata fieldsMetadata
    ) throws SQLException {
        if (tableDefns.get(connection, tableId) == null) {
            // Table does not yet exist, so attempt to create it ...
            try {
                create(config, connection, tableId, fieldsMetadata);
            } catch (final SQLException sqle) {
                log.warn("Create failed, will attempt amend if table already exists", sqle);
                try {
                    final TableDefinition newDefn = tableDefns.refresh(connection, tableId);
                    if (newDefn == null) {
                        throw sqle;
                    }
                } catch (final SQLException e) {
                    throw sqle;
                }
            }
        }
        return amendIfNecessary(config, connection, tableId, fieldsMetadata, config.maxRetries);
    }

    /**
     * @throws SQLException if CREATE failed
     */
    void create(
            final JdbcSinkConfig config,
            final Connection connection,
            final TableId tableId,
            final FieldsMetadata fieldsMetadata
    ) throws SQLException {
        if (!config.autoCreate) {
            throw new ConnectException(
                    String.format("Table %s is missing and auto-creation is disabled", tableId)
            );
        }
        final String sql = dbDialect.buildCreateTableStatement(tableId, fieldsMetadata.allFields.values());
        log.info("Creating table with sql: {}", sql);
        dbDialect.applyDdlStatements(connection, Collections.singletonList(sql));
    }

    /**
     * @return whether an ALTER was successfully performed
     * @throws SQLException if ALTER was deemed necessary but failed
     */
    boolean amendIfNecessary(
            final JdbcSinkConfig config,
            final Connection connection,
            final TableId tableId,
            final FieldsMetadata fieldsMetadata,
            final int maxRetries
    ) throws SQLException {
        // NOTE:
        //   The table might have extra columns defined (hopefully with default values), which is not
        //   a case we check for here.
        //   We also don't check if the data types for columns that do line-up are compatible.

        final TableDefinition tableDefn = tableDefns.get(connection, tableId);

        // FIXME: SQLite JDBC driver seems to not always return the PK column names?
        //    if (!tableMetadata.getPrimaryKeyColumnNames().equals(fieldsMetadata.keyFieldNames)) {
        //      throw new ConnectException(String.format(
        //          "Table %s has different primary key columns - database (%s), desired (%s)",
        //          tableName, tableMetadata.getPrimaryKeyColumnNames(), fieldsMetadata.keyFieldNames
        //      ));
        //    }

        final Set<SinkRecordField> missingFields = missingFields(
                fieldsMetadata.allFields.values(),
                tableDefn.columnNames()
        );

        if (missingFields.isEmpty()) {
            return false;
        }

        for (final SinkRecordField missingField : missingFields) {
            if (!missingField.isOptional() && missingField.defaultValue() == null) {
                throw new ConnectException(
                        "Cannot ALTER to add missing field " + missingField
                                + ", as it is not optional and does not have a default value"
                );
            }
        }

        if (!config.autoEvolve) {
            throw new ConnectException(String.format(
                    "Table %s is missing fields (%s) and auto-evolution is disabled",
                    tableId,
                    missingFields
            ));
        }

        final List<String> amendTableQueries = dbDialect.buildAlterTable(tableId, missingFields);
        log.info(
                "Amending table to add missing fields:{} maxRetries:{} with SQL: {}",
                missingFields,
                maxRetries,
                amendTableQueries
        );
        try {
            dbDialect.applyDdlStatements(connection, amendTableQueries);
        } catch (final SQLException sqle) {
            if (maxRetries <= 0) {
                throw new ConnectException(
                        String.format(
                                "Failed to amend table '%s' to add missing fields: %s",
                                tableId,
                                missingFields
                        ),
                        sqle
                );
            }
            log.warn("Amend failed, re-attempting", sqle);
            tableDefns.refresh(connection, tableId);
            // Perhaps there was a race with other tasks to add the columns
            return amendIfNecessary(
                    config,
                    connection,
                    tableId,
                    fieldsMetadata,
                    maxRetries - 1
            );
        }

        tableDefns.refresh(connection, tableId);
        return true;
    }

    Set<SinkRecordField> missingFields(
            final Collection<SinkRecordField> fields,
            final Set<String> dbColumnNames
    ) {
        final Set<SinkRecordField> missingFields = new HashSet<>();
        for (final SinkRecordField field : fields) {
            if (!dbColumnNames.contains(field.name())) {
                log.debug("Found missing field: {}", field);
                missingFields.add(field);
            }
        }

        if (missingFields.isEmpty()) {
            return missingFields;
        }

        // check if the missing fields can be located by ignoring case
        final Set<String> columnNamesLowerCase = new HashSet<>();
        for (final String columnName : dbColumnNames) {
            columnNamesLowerCase.add(columnName.toLowerCase());
        }

        if (columnNamesLowerCase.size() != dbColumnNames.size()) {
            log.warn(
                    "Table has column names that differ only by case. Original columns={}",
                    dbColumnNames
            );
        }

        final Set<SinkRecordField> missingFieldsIgnoreCase = new HashSet<>();
        for (final SinkRecordField missing : missingFields) {
            if (!columnNamesLowerCase.contains(missing.name().toLowerCase())) {
                missingFieldsIgnoreCase.add(missing);
            }
        }

        if (missingFieldsIgnoreCase.size() > 0) {
            log.info(
                    "Unable to find fields {} among column names {}",
                    missingFieldsIgnoreCase,
                    dbColumnNames
            );
        }

        return missingFieldsIgnoreCase;
    }
}
