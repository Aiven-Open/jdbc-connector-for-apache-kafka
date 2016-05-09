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
            throw new IllegalArgumentException("tableName parameter is not a valid.");
        }
        if (nonKeyColumns == null)
            throw new IllegalArgumentException("nonKeyColumns parameter is null.");

        if (keyColumns == null)
            throw new IllegalArgumentException("keyColumns parameter is null");

        if (nonKeyColumns.isEmpty() && keyColumns.isEmpty()) {
            throw new IllegalArgumentException("Illegal arguments. Both nonKey and key columns are empty");
        }

        final String questionMarks = Joiner.on(",").join(Collections.nCopies(nonKeyColumns.size() + keyColumns.size(), "?"));
        return String.format("INSERT INTO %s(%s) VALUES(%s)",
                tableName,
                Joiner.on(",").join(Iterables.concat(nonKeyColumns, keyColumns)), questionMarks);
    }
}
