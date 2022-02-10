/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
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

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.dialect.DatabaseDialects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(JdbcSinkTask.class);

    DatabaseDialect dialect;
    JdbcSinkConfig config;
    JdbcDbWriter writer;
    int remainingRetries;

    @Override
    public void start(final Map<String, String> props) {
        log.info("Starting JDBC Sink task");
        config = new JdbcSinkConfig(props);
        initWriter();
        remainingRetries = config.maxRetries;
    }

    void initWriter() {
        final String dialectName = config.getDialectName();
        if (dialectName != null && !dialectName.trim().isEmpty()) {
            dialect = DatabaseDialects.create(dialectName, config);
        } else {
            final String connectionUrl = config.getConnectionUrl();
            dialect = DatabaseDialects.findBestFor(connectionUrl, config);
        }
        final DbStructure dbStructure = new DbStructure(dialect);
        log.info("Initializing writer using SQL dialect: {}", dialect.getClass().getSimpleName());
        writer = new JdbcDbWriter(config, dialect, dbStructure);
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        final SinkRecord first = records.iterator().next();
        final int recordsCount = records.size();
        log.debug(
            "Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the "
                + "database...",
            recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset()
        );
        try {
            writer.write(records);
        } catch (final SQLException sqle) {
            log.warn(
                "Write of {} records failed, remainingRetries={}",
                records.size(),
                remainingRetries,
                sqle
            );
            String sqleAllMessages = "";
            for (final Throwable e : sqle) {
                sqleAllMessages += e + System.lineSeparator();
            }
            if (remainingRetries == 0) {
                throw new ConnectException(new SQLException(sqleAllMessages));
            } else {
                writer.closeQuietly();
                initWriter();
                remainingRetries--;
                context.timeout(config.retryBackoffMs);
                throw new RetriableException(new SQLException(sqleAllMessages));
            }
        }
        remainingRetries = config.maxRetries;
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> map) {
        // Not necessary
    }

    public void stop() {
        log.info("Stopping task");
        try {
            writer.closeQuietly();
        } finally {
            try {
                if (dialect != null) {
                    dialect.close();
                }
            } catch (final Throwable t) {
                log.warn("Error while closing the {} dialect: ", dialect.name(), t);
            } finally {
                dialect = null;
            }
        }
    }

    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }

}
