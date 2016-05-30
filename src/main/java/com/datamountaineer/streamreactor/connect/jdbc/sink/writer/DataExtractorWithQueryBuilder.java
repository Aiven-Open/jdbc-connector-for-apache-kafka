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
