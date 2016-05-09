/**
 * Copyright 2015 Datamountaineer.
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
 **/

package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;

import com.datamountaineer.streamreactor.connect.jdbc.sink.StructFieldsDataExtractor;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.PreparedStatementBinder;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Creates a sql statement for each record
 */
public final class SinglePreparedStatementBuilder implements PreparedStatementBuilder {

  private static final Logger logger = LoggerFactory.getLogger(SinglePreparedStatementBuilder.class);
  private static final Function<PreparedStatementBinder, String> fieldNamesFunc = new Function<PreparedStatementBinder, String>() {
    @Override
    public String apply(PreparedStatementBinder input) {
      return input.getFieldName();
    }
  };
  private final String tableName;
  private final StructFieldsDataExtractor fieldsExtractor;
  private final QueryBuilder queryBuilder;

  /**
   * @param tableName       - The name of the database tabled
   * @param fieldsExtractor - An instance of the SinkRecord fields value extractor
   */
  public SinglePreparedStatementBuilder(String tableName, StructFieldsDataExtractor fieldsExtractor, QueryBuilder queryBuilder) {
    this.tableName = tableName;
    this.fieldsExtractor = fieldsExtractor;
    this.queryBuilder = queryBuilder;
  }

  /**
   * Creates a PreparedStatement for each SinkRecord
   *
   * @param records    - The sequence of records to be inserted to the database
   * @param connection - The database connection instance
   * @return A sequence of PreparedStatement to be executed. It will batch the sql operation.
   */
  @Override
  public List<PreparedStatement> build(final Collection<SinkRecord> records, final Connection connection) throws SQLException {
    final List<PreparedStatement> statements = new ArrayList<>(records.size());
    for (final SinkRecord record : records) {
      logger.debug("Received record from topic:%s partition:%d and offset:$d", record.topic(), record.kafkaPartition(), record.kafkaOffset());
      if (record.value() == null || record.value().getClass() != Struct.class) {
        throw new IllegalArgumentException("The SinkRecord payload should be of type Struct");
      }

      final StructFieldsDataExtractor.PreparedStatementBinders binders = fieldsExtractor.get((Struct) record.value());

      if (!binders.isEmpty()) {
        final List<String> nonKeyColumnsName = Lists.transform(binders.getNonKeyColumns(), fieldNamesFunc);
        final List<String> keyColumnsName = Lists.transform(binders.getKeyColumns(), fieldNamesFunc);
        final String query = queryBuilder.build(tableName, nonKeyColumnsName, keyColumnsName);
        final PreparedStatement statement = connection.prepareStatement(query);
        PreparedStatementBindData.apply(statement,
                Iterables.concat(binders.getNonKeyColumns(), binders.getKeyColumns()));
        statements.add(statement);
      }
    }
    return statements;
  }

  @Override
  public boolean isBatching() {
    return false;
  }
}
