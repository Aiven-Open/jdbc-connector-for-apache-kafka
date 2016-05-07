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

import com.google.common.base.Joiner;
import java.util.Collections;
import java.util.List;

/**
 * Prepares an instance of PrepareStatement for inserting into the given table the fields present in the map
 */
public final class BuildInsertQuery {
    /**
     * Creates the insert  SQL statement for the given table and given columns
     *
     * @param tableName - The target database table
     * @param columns   - The list of columns for which values would be inserted
     * @return - The SQL insert statement used to create the PreparedStatement
     */
    public static String get(final String tableName, final List<String> columns) {
        if (tableName == null || tableName.trim().length() == 0)
            throw new IllegalArgumentException("tableName parameter is not a valid.");
        if (columns == null || columns.isEmpty())
            throw new IllegalArgumentException("columns parameter is not valid.");

        final String questionMarks = Joiner.on(",").join(Collections.nCopies(columns.size(), "?"));
        return String.format("INSERT INTO %s(%s) VALUES(%s)", tableName, Joiner.on(",").join(columns), questionMarks);
    }
}
