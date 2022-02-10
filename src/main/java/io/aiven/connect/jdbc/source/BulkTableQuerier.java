/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2015 Confluent Inc.
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

package io.aiven.connect.jdbc.source;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.source.SchemaMapping.FieldSetter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BulkTableQuerier always returns the entire table.
 */
public class BulkTableQuerier extends TableQuerier {
    private static final Logger log = LoggerFactory.getLogger(BulkTableQuerier.class);

    public BulkTableQuerier(
        final DatabaseDialect dialect,
        final QueryMode mode,
        final String name,
        final String topicPrefix
    ) {
        super(dialect, mode, name, topicPrefix);
    }

    @Override
    protected void createPreparedStatement(final Connection db) throws SQLException {
        switch (mode) {
            case TABLE:
                final String queryStr = dialect.expressionBuilder().append("SELECT * FROM ")
                    .append(tableId).toString();
                log.debug("{} prepared SQL query: {}", this, queryStr);
                stmt = dialect.createPreparedStatement(db, queryStr);
                break;
            case QUERY:
                log.debug("{} prepared SQL query: {}", this, query);
                stmt = dialect.createPreparedStatement(db, query);
                break;
            default:
                throw new ConnectException("Unknown mode: " + mode);
        }
    }

    @Override
    protected ResultSet executeQuery() throws SQLException {
        return stmt.executeQuery();
    }

    @Override
    public SourceRecord extractRecord() throws SQLException {
        final Struct record = new Struct(schemaMapping.schema());
        for (final FieldSetter setter : schemaMapping.fieldSetters()) {
            try {
                setter.setField(record, resultSet);
            } catch (final IOException e) {
                log.warn("Ignoring record because processing failed:", e);
            } catch (final SQLException e) {
                log.warn("Ignoring record due to SQL error:", e);
            }
        }
        // TODO: key from primary key? partition?
        final String topic;
        final Map<String, String> partition;
        switch (mode) {
            case TABLE:
                final String name = tableId.tableName(); // backwards compatible
                partition = Collections.singletonMap(JdbcSourceConnectorConstants.TABLE_NAME_KEY, name);
                topic = topicPrefix + name;
                break;
            case QUERY:
                partition = Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
                    JdbcSourceConnectorConstants.QUERY_NAME_VALUE
                );
                topic = topicPrefix;
                break;
            default:
                throw new ConnectException("Unexpected query mode: " + mode);
        }
        return new SourceRecord(partition, null, topic, record.schema(), record);
    }

    @Override
    public String toString() {
        return "BulkTableQuerier{" + "table='" + tableId + '\'' + ", query='" + query + '\''
            + ", topicPrefix='" + topicPrefix + '\'' + '}';
    }

}
