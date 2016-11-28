/*
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

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.confluent.connect.jdbc.sink.dialect.DbDialect;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;

public class BufferedRecords {
  private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);

  private final String tableName;
  private final JdbcSinkConfig config;
  private final DbDialect dbDialect;
  private final DbStructure dbStructure;
  private final Connection connection;

  private List<SinkRecord> records = new ArrayList<>();
  private SchemaPair currentSchemaPair;
  private FieldsMetadata fieldsMetadata;
  private PreparedStatement preparedStatement;
  private PreparedStatementBinder preparedStatementBinder;

  public BufferedRecords(JdbcSinkConfig config, String tableName, DbDialect dbDialect, DbStructure dbStructure, Connection connection) {
    this.tableName = tableName;
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;
    this.connection = connection;
  }

  public List<SinkRecord> add(SinkRecord record) throws SQLException {
    final SchemaPair schemaPair = new SchemaPair(record.keySchema(), record.valueSchema());

    if (currentSchemaPair == null) {
      currentSchemaPair = schemaPair;
      // re-initialize everything that depends on the record schema
      fieldsMetadata = FieldsMetadata.extract(tableName, config.pkMode, config.pkFields, config.fieldsWhitelist, currentSchemaPair);
      dbStructure.createOrAmendIfNecessary(config, connection, tableName, fieldsMetadata);
      final String insertSql = getInsertSql();
      log.debug("{} sql: {}", config.insertMode, insertSql);
      close();
      preparedStatement = connection.prepareStatement(insertSql);
      preparedStatementBinder = new PreparedStatementBinder(preparedStatement, config.pkMode, schemaPair, fieldsMetadata);
    }

    final List<SinkRecord> flushed;
    if (currentSchemaPair.equals(schemaPair)) {
      // Continue with current batch state
      records.add(record);
      if (records.size() >= config.batchSize) {
        flushed = flush();
      } else {
        flushed = Collections.emptyList();
      }
    } else {
      // Each batch needs to have the same SchemaPair, so get the buffered records out, reset state and re-attempt the add
      flushed = flush();
      currentSchemaPair = null;
      flushed.addAll(add(record));
    }
    return flushed;
  }

  public List<SinkRecord> flush() throws SQLException {
    if (records.isEmpty()) {
      return new ArrayList<>();
    }
    for (SinkRecord record : records) {
      preparedStatementBinder.bindRecord(record);
    }
    int totalUpdateCount = 0;
    for (int updateCount : preparedStatement.executeBatch()) {
      totalUpdateCount += updateCount;
    }
    if (totalUpdateCount != records.size()) {
      switch (config.insertMode) {
        case INSERT:
          throw new ConnectException(String.format("Update count (%d) did not sum up to total number of records inserted (%d)",
                                                   totalUpdateCount, records.size()));
        case UPSERT:
          log.trace("Upserted records:{} resulting in in totalUpdateCount:{}", records.size(), totalUpdateCount);
      }
    }

    final List<SinkRecord> flushedRecords = records;
    records = new ArrayList<>();
    return flushedRecords;
  }

  public void close() throws SQLException {
    if (preparedStatement != null) {
      preparedStatement.close();
      preparedStatement = null;
    }
  }

  private String getInsertSql() {
    switch (config.insertMode) {
      case INSERT:
        return dbDialect.getInsert(tableName, fieldsMetadata.keyFieldNames, fieldsMetadata.nonKeyFieldNames);
      case UPSERT:
        if (fieldsMetadata.keyFieldNames.isEmpty()) {
          throw new ConnectException(String.format(
              "Write to table '%s' in UPSERT mode requires key field names to be known, check the primary key configuration", tableName
          ));
        }
        return dbDialect.getUpsertQuery(tableName, fieldsMetadata.keyFieldNames, fieldsMetadata.nonKeyFieldNames);
      default:
        throw new ConnectException("Invalid insert mode");
    }
  }
}
