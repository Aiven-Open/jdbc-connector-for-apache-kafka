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

import com.datamountaineer.streamreactor.connect.jdbc.sink.RecordDataExtractor;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.PreparedStatementBinder;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Creates an iterator responsible for returning the sql statement for the group of records sharing the same target table
 * and columns populated.
 */
public final class PreparedStatementContextIterable {
  private static final Logger logger = LoggerFactory.getLogger(PreparedStatementContextIterable.class);

  private static final Function<PreparedStatementBinder, String> fieldNamesFunc = new Function<PreparedStatementBinder, String>() {
    @Override
    public String apply(PreparedStatementBinder input) {
      return input.getFieldName();
    }
  };

  private final int batchSize;
  private final Map<String, DataExtractorWithQueryBuilder> topicsMap;

  /**
   * Creates a new instance of PreparedStatementContextIterable
   *
   * @param topicsMap - A map between topics to the payload field values extractor and the query builder strategy
   * @param batchSize - The maximum amount of values to be sent to the RDBMS in one batch execution
   */
  public PreparedStatementContextIterable(final Map<String, DataExtractorWithQueryBuilder> topicsMap,
                                          final int batchSize) {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("Invalid batchSize specified. The value has to be a positive and non zero integer.");
    }
    if (topicsMap == null || topicsMap.size() == 0) {
      throw new IllegalArgumentException("Invalid fieldsExtractorMap provided.");
    }
    this.topicsMap = topicsMap;
    this.batchSize = batchSize;
  }

  /**
   * @param records - The sequence of records to be inserted to the database
   * @return A sequence of PreparedStatementContext to be executed. It will batch the sql operation.
   */
  public Iterator<PreparedStatementContext> iterator(final Collection<SinkRecord> records) {

    return new Iterator<PreparedStatementContext>() {
      final Iterator<SinkRecord> iterator = records.iterator();

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public PreparedStatementContext next() {
        final Map<String, PreparedStatementData> mapStatements = new HashMap<>();
        final TablesToColumnUsageState state = new TablesToColumnUsageState();

        int batch = batchSize;
        while (batch > 0 && iterator.hasNext()) {
          final SinkRecord record = iterator.next();
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

          final String topic = record.topic().toLowerCase();
          if (!topicsMap.containsKey(topic)) {
            logger.warn(String.format("For topic %s there is no mapping.Skipping record at partition %d and offset %d",
                    record.topic(),
                    record.kafkaPartition(),
                    record.kafkaOffset()));
            continue;
          }

          DataExtractorWithQueryBuilder extractorWithQueryBuilder = topicsMap.get(topic);
          final RecordDataExtractor fieldsDataExtractor = extractorWithQueryBuilder.getDataExtractor();
          final Struct struct = (Struct) record.value();
          final List<PreparedStatementBinder> binders = fieldsDataExtractor.get(struct, record);

          if (!binders.isEmpty()) {
            final String tableName = fieldsDataExtractor.getTableName();
            state.trackUsage(tableName, binders);

            final List<String> nonKeyColumnsName = new LinkedList<>();
            final List<String> keyColumnsName = new LinkedList<>();
            for (PreparedStatementBinder b : binders) {
              if (b.isPrimaryKey()) {
                keyColumnsName.add(b.getFieldName());
              } else {
                nonKeyColumnsName.add(b.getFieldName());
              }
            }

            final String statementKey = Joiner.on("").join(Iterables.concat(nonKeyColumnsName, keyColumnsName));

            if (!mapStatements.containsKey(statementKey)) {
              final String query = extractorWithQueryBuilder
                      .getQueryBuilder()
                      .build(tableName, nonKeyColumnsName, keyColumnsName);
              mapStatements.put(statementKey, new PreparedStatementData(query, Lists.<Iterable<PreparedStatementBinder>>newLinkedList()));
            }
            final PreparedStatementData statementData = mapStatements.get(statementKey);
            statementData.addEntryBinders(binders);
            batch--;
          }
        }

        return new PreparedStatementContext(mapStatements.values(), state.getState());
      }

      @Override
      public void remove() {
        throw new AbstractMethodError();
      }
    };
  }
}
