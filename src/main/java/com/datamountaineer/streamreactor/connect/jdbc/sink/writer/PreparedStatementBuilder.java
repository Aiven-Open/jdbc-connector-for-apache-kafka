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
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldsMappings;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkSettings;
import com.google.common.collect.Maps;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface PreparedStatementBuilder {

  /**
   * Build a list of prepared statements the sink records against this connection.
   *
   * @param records    The sinkRecords to create prepared statements for.
   * @param connection The database connection to create the prepared statements on.
   * @return A list of prepared statements for the sink records.
   */
  List<PreparedStatement> build(final Collection<SinkRecord> records,
                                final Connection connection) throws SQLException;

  boolean isBatching();
}

final class PreparedStatementBuilderHelper {
  /**
   * Creates a new instance of PrepareStatementBuilder
   *
   * @param settings - Instance of the Jdbc sink settings
   * @return - Returns an instance of PreparedStatementBuilder depending on the settings asking for batched or
   * non-batched inserts
   */
  public static PreparedStatementBuilder from(final JdbcSinkSettings settings) {
    Map<String, StructFieldsDataExtractor> map = Maps.newHashMap();
    for (final FieldsMappings tm : settings.getMappings()) {
      final StructFieldsDataExtractor fieldsValuesExtractor = new StructFieldsDataExtractor(tm);

      map.put(tm.getIncomingTopic().toLowerCase(), fieldsValuesExtractor);
    }

    final QueryBuilder queryBuilder = QueryBuilderHelper.from(settings);

    if (settings.isBatching()) {
      return new BatchedPreparedStatementBuilder(map, queryBuilder);
    }

    return new SinglePreparedStatementBuilder(map, queryBuilder);
  }
}


