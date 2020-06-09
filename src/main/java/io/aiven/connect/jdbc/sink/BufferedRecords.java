/*
 * Copyright 2019 Aiven Oy
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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.dialect.DatabaseDialect.StatementBinder;
import io.aiven.connect.jdbc.sink.metadata.FieldsMetadata;
import io.aiven.connect.jdbc.sink.metadata.SchemaPair;
import io.aiven.connect.jdbc.util.ColumnId;
import io.aiven.connect.jdbc.util.TableId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.aiven.connect.jdbc.sink.JdbcSinkConfig.InsertMode.MULTI;

public class BufferedRecords {
    private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);

    private final TableId tableId;
    private final JdbcSinkConfig config;
    private final DatabaseDialect dbDialect;
    private final DbStructure dbStructure;
    private final Connection connection;

    private List<SinkRecord> records = new ArrayList<>();
    private SchemaPair currentSchemaPair;
    private FieldsMetadata fieldsMetadata;
    private PreparedStatement preparedStatement;
    private StatementBinder preparedStatementBinder;

    public BufferedRecords(
        final JdbcSinkConfig config,
        final TableId tableId,
        final DatabaseDialect dbDialect,
        final DbStructure dbStructure,
        final Connection connection
    ) {
        this.tableId = tableId;
        this.config = config;
        this.dbDialect = dbDialect;
        this.dbStructure = dbStructure;
        this.connection = connection;
    }

    public List<SinkRecord> add(final SinkRecord record) throws SQLException {
        final SchemaPair schemaPair = new SchemaPair(
            record.keySchema(),
            record.valueSchema()
        );

        if (currentSchemaPair == null) {
            currentSchemaPair = schemaPair;
            // re-initialize everything that depends on the record schema
            fieldsMetadata = FieldsMetadata.extract(
                tableId.tableName(),
                config.pkMode,
                config.pkFields,
                config.fieldsWhitelist,
                currentSchemaPair
            );
            dbStructure.createOrAmendIfNecessary(
                config,
                connection,
                tableId,
                fieldsMetadata
            );

            final String sql;
            if (config.insertMode == MULTI && records.isEmpty())  {
                sql = getFirstMultirowInsertSql();
            } else {
                sql = getInsertSql();
            }

            log.debug(
                "{} sql: {}",
                config.insertMode,
                sql
            );
            close();
            preparedStatement = connection.prepareStatement(sql);
            preparedStatementBinder = dbDialect.statementBinder(
                preparedStatement,
                config.pkMode,
                schemaPair,
                fieldsMetadata,
                config.insertMode
            );
        }

        final List<SinkRecord> flushed;
        if (currentSchemaPair.equals(schemaPair)) {
            // Continue with current batch state
            records.add(record);
            if (records.size() >= config.batchSize) {
                log.debug("Flushing buffered records after exceeding configured batch size {}.",
                    config.batchSize);
                flushed = flush();
            } else {
                flushed = Collections.emptyList();
            }
        } else {
            // Each batch needs to have the same SchemaPair, so get the buffered records out, reset
            // state and re-attempt the add
            log.debug("Flushing buffered records after due to unequal schema pairs: "
                + "current schemas: {}, next schemas: {}", currentSchemaPair, schemaPair);
            flushed = flush();
            currentSchemaPair = null;
            flushed.addAll(add(record));
        }
        return flushed;
    }

    public List<SinkRecord> flush() throws SQLException {
        if (records.isEmpty()) {
            log.debug("Records is empty");
            return new ArrayList<>();
        }
        log.debug("Flushing {} buffered records", records.size());
        for (final SinkRecord record : records) {
            preparedStatementBinder.bindRecord(record);
        }
        int totalUpdateCount = 0;
        boolean successNoInfo = false;
        for (final int updateCount : preparedStatement.executeBatch()) {
            if (updateCount == Statement.SUCCESS_NO_INFO) {
                successNoInfo = true;
                continue;
            }
            totalUpdateCount += updateCount;
        }
        if (totalUpdateCount != records.size() && !successNoInfo) {
            switch (config.insertMode) {
                case INSERT:
                    throw new ConnectException(String.format(
                        "Update count (%d) did not sum up to total number of records inserted (%d)",
                        totalUpdateCount,
                        records.size()
                    ));
                case MULTI:
                case UPSERT:
                case UPDATE:
                    log.debug(
                        "{} records:{} resulting in in totalUpdateCount:{}",
                        config.insertMode,
                        records.size(),
                        totalUpdateCount
                    );
                    break;
                default:
                    throw new ConnectException("Unknown insert mode: " + config.insertMode);
            }
        }
        if (successNoInfo) {
            log.info(
                "{} records:{} , but no count of the number of rows it affected is available",
                config.insertMode,
                records.size()
            );
        }

        final List<SinkRecord> flushedRecords = records;
        records = new ArrayList<>();
        return flushedRecords;
    }

    public void close() throws SQLException {
        log.info("Closing BufferedRecords with preparedStatement: {}", preparedStatement);
        if (preparedStatement != null) {
            preparedStatement.close();
            preparedStatement = null;
        }
    }

    private String getFirstMultirowInsertSql() {
        if (config.insertMode != MULTI) {
            throw new ConnectException(String.format(
                    "Multi-row first insert SQL unsupported by insert mode %s",
                    config.insertMode
            ));
        }
        try {
            return dbDialect.buildFirstMultiInsertStatement(
                    tableId,
                    asColumns(fieldsMetadata.keyFieldNames),
                    asColumns(fieldsMetadata.nonKeyFieldNames)
            );
        } catch (final UnsupportedOperationException e) {
            throw new ConnectException(String.format(
                    "Write to table '%s' in MULTI mode is not supported with the %s dialect.",
                    tableId,
                    dbDialect.name()
            ));
        }
    }

    private String getInsertSql() {
        switch (config.insertMode) {
            case INSERT:
                return dbDialect.buildInsertStatement(
                    tableId,
                    asColumns(fieldsMetadata.keyFieldNames),
                    asColumns(fieldsMetadata.nonKeyFieldNames)
                );
            case MULTI:
                return dbDialect.buildMultiInsertStatement(
                        tableId,
                        asColumns(fieldsMetadata.keyFieldNames),
                        asColumns(fieldsMetadata.nonKeyFieldNames)
                );
            case UPSERT:
                if (fieldsMetadata.keyFieldNames.isEmpty()) {
                    throw new ConnectException(String.format(
                        "Write to table '%s' in UPSERT mode requires key field names to be known, check the"
                            + " primary key configuration",
                        tableId
                    ));
                }
                try {
                    return dbDialect.buildUpsertQueryStatement(
                        tableId,
                        asColumns(fieldsMetadata.keyFieldNames),
                        asColumns(fieldsMetadata.nonKeyFieldNames)
                    );
                } catch (final UnsupportedOperationException e) {
                    throw new ConnectException(String.format(
                        "Write to table '%s' in UPSERT mode is not supported with the %s dialect.",
                        tableId,
                        dbDialect.name()
                    ));
                }
            case UPDATE:
                return dbDialect.buildUpdateStatement(
                    tableId,
                    asColumns(fieldsMetadata.keyFieldNames),
                    asColumns(fieldsMetadata.nonKeyFieldNames)
                );
            default:
                throw new ConnectException("Invalid insert mode");
        }
    }

    private Collection<ColumnId> asColumns(final Collection<String> names) {
        return names.stream()
            .map(name -> new ColumnId(tableId, name))
            .collect(Collectors.toList());
    }
}
