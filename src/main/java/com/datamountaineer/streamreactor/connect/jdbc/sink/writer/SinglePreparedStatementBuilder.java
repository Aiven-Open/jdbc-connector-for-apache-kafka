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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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

  private final Map<String, StructFieldsDataExtractor> fieldsDataExtractorMap;
  private final QueryBuilder queryBuilder;


  public SinglePreparedStatementBuilder(Map<String, StructFieldsDataExtractor> fieldsDataExtractorMap,
                                        QueryBuilder queryBuilder) {
    this.fieldsDataExtractorMap = fieldsDataExtractorMap;
    this.queryBuilder = queryBuilder;
  }

  /**
   * Creates a PreparedStatement for each SinkRecord
   *
   * @param records    - The sequence of records to be inserted to the database
   * @return A sequence of PreparedStatement to be executed. It will batch the sql operation.
   */
  @Override
  public PreparedStatementContext build(final Collection<SinkRecord> records) {
    final List<PreparedStatementData> statements = new ArrayList<>(records.size());
    final TablesToColumnUsageState tablesToColumnsState = new TablesToColumnUsageState();

    for (final SinkRecord record : records) {
      logger.debug("Received record from topic:%s partition:%d and offset:$d",
              record.topic(),
              record.kafkaPartition(),
              record.kafkaOffset());

      if (record.value() == null || record.value().getClass() != Struct.class) {
        final String msg = String.format("On topic %s partition %d and offset %d the payload is not of type struct",
                record.topic(),
                record.kafkaPartition(),
                record.kafkaOffset());
        logger.error(msg);
        throw new IllegalArgumentException(msg);
      }

      if (!fieldsDataExtractorMap.containsKey(record.topic().toLowerCase())) {
        logger.warn(String.format("For topic %s there is no mapping.Skipping record at partition %d and offset %d",
                record.topic(),
                record.kafkaPartition(),
                record.kafkaOffset()));
        continue;
      }
      final Struct struct = (Struct) record.value();
      final StructFieldsDataExtractor fieldsDataExtractor = fieldsDataExtractorMap.get(record.topic().toLowerCase());
      final StructFieldsDataExtractor.PreparedStatementBinders binders = fieldsDataExtractor.get(struct, record);

      if (!binders.isEmpty()) {
        final String tableName = fieldsDataExtractor.getTableName();
        tablesToColumnsState.trackUsage(tableName, binders);
        final List<String> nonKeyColumnsName = Lists.transform(binders.getNonKeyColumns(), fieldNamesFunc);
        final List<String> keyColumnsName = Lists.transform(binders.getKeyColumns(), fieldNamesFunc);
        final String query = queryBuilder.build(tableName, nonKeyColumnsName, keyColumnsName);
        //final PreparedStatement statement = connection.prepareStatement(query);
        //PreparedStatementBindData.apply(statement, Iterables.concat(binders.getNonKeyColumns(), binders.getKeyColumns()));
        final List<Iterable<PreparedStatementBinder>> entryBinders = new ArrayList<>();
        entryBinders.add(Iterables.concat(binders.getNonKeyColumns(), binders.getKeyColumns()));
        final PreparedStatementData data = new PreparedStatementData(query, entryBinders);
        statements.add(data);
      }
    }

    return new PreparedStatementContext(statements, tablesToColumnsState.getState());
  }

  @Override
  public boolean isBatching() {
    return false;
  }
}
