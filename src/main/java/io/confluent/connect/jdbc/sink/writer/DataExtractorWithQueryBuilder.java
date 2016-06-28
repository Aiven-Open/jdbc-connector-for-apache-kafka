package io.confluent.connect.jdbc.sink.writer;

import io.confluent.connect.jdbc.sink.RecordDataExtractor;

/**
 * Holds the instance to the SinkRecord values extractor and the query builder strategy.
 */
public class DataExtractorWithQueryBuilder {
  private final QueryBuilder queryBuilder;
  private final RecordDataExtractor dataExtractor;

  public DataExtractorWithQueryBuilder(QueryBuilder queryBuilder, RecordDataExtractor dataExtractor) {
    this.queryBuilder = queryBuilder;
    this.dataExtractor = dataExtractor;
  }

  public QueryBuilder getQueryBuilder() {
    return queryBuilder;
  }

  public RecordDataExtractor getDataExtractor() {
    return dataExtractor;
  }
}
