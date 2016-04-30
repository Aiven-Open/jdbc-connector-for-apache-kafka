package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;


import com.google.common.base.Joiner;

import java.util.Collections;
import java.util.List;

/**
 * Prepares an instance of PrepareStatement for inserting into the given table the fields present in the map
 */
public final class BuildInsertQuery {
    public static String apply(final String tableName, final List<String> columns) {
        if (tableName == null || tableName.trim().length() == 0)
            throw new IllegalArgumentException("tableName parameter is not a valid.");
        if (columns == null || columns.isEmpty())
            throw new IllegalArgumentException("columns parameter is not valid.");

        final String questionMarks = Joiner.on(",").join(Collections.nCopies(columns.size(), "?"));
        return String.format("INSERT INTO %s(%s) VALUES(%s)", tableName, Joiner.on(",").join(columns), questionMarks);
    }
}
