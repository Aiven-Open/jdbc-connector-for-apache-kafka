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

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

import java.util.Collections;
import java.util.List;

public final class InsertQueryBuilder implements QueryBuilder {

  @Override
  public String build(final String tableName,
                      final List<String> nonKeyColumns,
                      final List<String> keyColumns) {
    if (tableName == null || tableName.trim().length() == 0) {
      throw new IllegalArgumentException("tableName parameter is not a valid table name.");
    }
    if (nonKeyColumns == null)
      throw new IllegalArgumentException("nonKeyColumns parameter is null.");

    if (keyColumns == null)
      throw new IllegalArgumentException("keyColumns parameter is null");

    if (nonKeyColumns.isEmpty() && keyColumns.isEmpty()) {
      throw new IllegalArgumentException("Illegal arguments. Both nonKeyColumns and keyColumns are empty");
    }

    final String questionMarks = Joiner.on(",").join(Collections.nCopies(nonKeyColumns.size() + keyColumns.size(), "?"));
    return String.format("INSERT INTO %s(%s) VALUES(%s)",
            tableName,
            Joiner.on(",").join(Iterables.concat(nonKeyColumns, keyColumns)), questionMarks);
  }
}
