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
import java.util.NoSuchElementException;

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
      private final Iterator<SinkRecord> iterator = records.iterator();
      private final Map<String, PreparedStatementData> mapStatements = new HashMap<>();
      private final TablesToColumnUsageState state = new TablesToColumnUsageState();

      private PreparedStatementData current = null;

      @Override
      public boolean hasNext() {
        return iterator.hasNext() || mapStatements.size() > 0;
      }

      private String getBatchSizeMatch() {
        for (Map.Entry<String, PreparedStatementData> entry : mapStatements.entrySet()) {
          if (entry.getValue().getBinders().size() == batchSize) {
            return entry.getKey();
          }
        }
        return null;
      }

      @Override
      public PreparedStatementContext next() {

        if (mapStatements.size() == 0 && !iterator.hasNext()) {
          throw new NoSuchElementException();
        }

        final String keyToRemove = getBatchSizeMatch();
        if (keyToRemove != null) {
          final PreparedStatementData data = mapStatements.remove(keyToRemove);
          return new PreparedStatementContext(data, state.getState());
        }

        //no more batch size
        if (!iterator.hasNext()) {
          //return each entries until we drain them
          String firstKey = mapStatements.keySet().iterator().next();
          final PreparedStatementData data = mapStatements.remove(firstKey);
          return new PreparedStatementContext(data, state.getState());
        }

        //there are more entries in the iterator
        while (iterator.hasNext()) {
          final SinkRecord record = iterator.next();

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
            logger.warn("For topic {} there is no mapping. Skipping record at partition {} and offset {}",
                    record.topic(),
                    record.kafkaPartition(),
                    record.kafkaOffset());
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

            final String statementKey = tableName + Joiner.on("").join(Iterables.concat(nonKeyColumnsName, keyColumnsName));

            if (!mapStatements.containsKey(statementKey)) {
              final String query = extractorWithQueryBuilder
                      .getQueryBuilder()
                      .build(tableName, nonKeyColumnsName, keyColumnsName);
              mapStatements.put(statementKey, new PreparedStatementData(query, Lists.<Iterable<PreparedStatementBinder>>newLinkedList()));
            }
            final PreparedStatementData statementData = mapStatements.get(statementKey);
            statementData.addEntryBinders(binders);
            //we have reached a full batch size
            //we could short circuit here
            if (statementData.getBinders().size() == batchSize) {
              break;
            }
          }
        }

        //what if because of topic mappings we got no more entries
        if (mapStatements.size() == 0) {
          throw new NoSuchElementException();
        }

        final String maxBatchKeyToRemove = getBatchSizeMatch();
        if (maxBatchKeyToRemove != null) {
          final PreparedStatementData data = mapStatements.remove(maxBatchKeyToRemove);
          return new PreparedStatementContext(data, state.getState());
        }

        //return each entries until we drain them
        String firstKey = mapStatements.keySet().iterator().next();
        final PreparedStatementData data = mapStatements.remove(firstKey);
        return new PreparedStatementContext(data, state.getState());
      }

      @Override
      public void remove() {
        throw new AbstractMethodError();
      }
    };
  }
}
