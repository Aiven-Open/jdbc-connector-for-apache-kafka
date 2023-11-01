/*
 * Copyright 2019 Aiven Oy and jdbc-connector-for-apache-kafka project contributors
 * Copyright 2018 Confluent Inc.
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;

import io.aiven.connect.jdbc.dialect.DatabaseDialect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountQuerier extends TableQuerier {
    private static final Logger log = LoggerFactory.getLogger(CountQuerier.class);

    public CountQuerier(final DatabaseDialect dialect, final QueryMode mode, final String nameOrQuery) {
        super(dialect, mode, nameOrQuery, null);
    }

    @Override
    protected void createPreparedStatement(final Connection db) throws SQLException {
        final String queryStr;
        switch (mode) {
            case TABLE:
                queryStr = dialect.expressionBuilder().append("SELECT count(*) FROM ")
                        .append(tableId).toString();
                break;
            case QUERY:
                queryStr = dialect.expressionBuilder().append("SELECT count(*) FROM ")
                        .append("(")
                        .append(query)
                        .append(") as count_query")
                        .toString();
                break;
            default:
                throw new ConnectException("Unknown mode: " + mode);
        }
        log.debug("{} prepared SQL query: {}", this, queryStr);
        stmt = dialect.createPreparedStatement(db, queryStr);
    }

    @Override
    protected ResultSet executeQuery() throws SQLException {
        return stmt.executeQuery();
    }

    @Override
    public SourceRecord extractRecord() throws SQLException {
        throw new UnsupportedOperationException("CountQuerier does not support extracting records");
    }

    public Long count() throws SQLException {
        final ResultSet resultSet = this.executeQuery();
        if (resultSet.next()) {
            return resultSet.getLong(1);
        }
        return null;
    }

}
