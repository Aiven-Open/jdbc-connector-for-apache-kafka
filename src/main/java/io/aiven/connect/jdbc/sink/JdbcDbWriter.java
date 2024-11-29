/*
 * Copyright 2020 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
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

import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.util.CachedConnectionProvider;
import io.aiven.connect.jdbc.util.TableId;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

public class JdbcDbWriter {

    private static final Logger log = LoggerFactory.getLogger(JdbcDbWriter.class);

    private final JdbcSinkConfig config;
    private final DatabaseDialect dbDialect;
    private final DbStructure dbStructure;
    final CachedConnectionProvider cachedConnectionProvider;

    public JdbcDbWriter(final JdbcSinkConfig config, final DatabaseDialect dbDialect, final DbStructure dbStructure) {
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

    public void write(final Collection<SinkRecord> records) throws SQLException {
        final Connection connection = cachedConnectionProvider.getConnection();
        final BufferManager bufferManager = new BufferManager(config, dbDialect, dbStructure, connection);

        for (SinkRecord record : records) {
            bufferManager.addRecord(record);
        }

        bufferManager.flushAndClose();
        connection.commit();
    }

    public void closeQuietly() {
        cachedConnectionProvider.close();
    }

    TableId destinationTable(final String topic) {
        final String tableName = TableNameGenerator.generateTableName(config, topic);
        return dbDialect.parseTableIdentifier(tableName);
    }

    public String generateTableNameFor(final String topic) {
        return TableNameGenerator.generateTableName(config, topic);
    }
}
