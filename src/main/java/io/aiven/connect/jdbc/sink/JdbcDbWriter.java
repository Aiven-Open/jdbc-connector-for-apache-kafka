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
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.util.CachedConnectionProvider;
import io.aiven.connect.jdbc.util.TableId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcDbWriter {

    private static final Logger log = LoggerFactory.getLogger(JdbcDbWriter.class);

    private static final Pattern NORMALIZE_TABLE_NAME_FOR_TOPIC = Pattern.compile("[^a-zA-Z0-9_]");

    private final JdbcSinkConfig config;

    private final DatabaseDialect dbDialect;

    private final DbStructure dbStructure;

    final CachedConnectionProvider cachedConnectionProvider;

    JdbcDbWriter(final JdbcSinkConfig config, final DatabaseDialect dbDialect, final DbStructure dbStructure) {
        this.config = config;
        this.dbDialect = dbDialect;
        this.dbStructure = dbStructure;

        this.cachedConnectionProvider = new CachedConnectionProvider(this.dbDialect) {
            @Override
            protected void onConnect(final Connection connection) throws SQLException {
                log.info("JdbcDbWriter Connected");
                connection.setAutoCommit(false);
            }
        };
    }

    void write(final Collection<SinkRecord> records) throws SQLException {
        final Connection connection = cachedConnectionProvider.getConnection();

        final Map<TableId, BufferedRecords> bufferByTable = new HashMap<>();
        for (final SinkRecord record : records) {
            final TableId tableId = destinationTable(record.topic());
            BufferedRecords buffer = bufferByTable.get(tableId);
            if (buffer == null) {
                buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
                bufferByTable.put(tableId, buffer);
            }
            buffer.add(record);
        }
        for (final Map.Entry<TableId, BufferedRecords> entry : bufferByTable.entrySet()) {
            final TableId tableId = entry.getKey();
            final BufferedRecords buffer = entry.getValue();
            log.debug("Flushing records in JDBC Writer for table ID: {}", tableId);
            buffer.flush();
            buffer.close();
        }
        connection.commit();
    }

    void closeQuietly() {
        cachedConnectionProvider.close();
    }

    TableId destinationTable(final String topic) {
        final String tableName = generateTableNameFor(topic);
        return dbDialect.parseTableIdentifier(tableName);
    }

    public String generateTableNameFor(final String topic) {
        String tableName = config.tableNameFormat.replace("${topic}", topic);
        if (config.tableNameNormalize) {
            tableName = NORMALIZE_TABLE_NAME_FOR_TOPIC.matcher(tableName).replaceAll("_");
        }
        if (!config.topicsToTablesMapping.isEmpty()) {
            tableName = config.topicsToTablesMapping.getOrDefault(topic, "");
        }
        if (tableName.isEmpty()) {
            final String errorMessage =
                    String.format(
                            "Destination table for the topic: '%s' "
                                    + "couldn't be found in the topics to tables mapping: '%s' "
                                    + "and couldn't be generated for the format string '%s'",
                            topic,
                            config.topicsToTablesMapping
                                    .entrySet()
                                    .stream()
                                    .map(e -> String.join("->", e.getKey(), e.getValue()))
                                    .collect(Collectors.joining(",")),
                            config.tableNameFormat);
            throw new ConnectException(errorMessage);
        }
        return tableName;
    }

}
