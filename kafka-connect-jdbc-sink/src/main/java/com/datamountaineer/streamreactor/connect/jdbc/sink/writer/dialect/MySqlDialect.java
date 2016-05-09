package com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

import java.util.Collections;
import java.util.List;

/**
 * Provides support for MySql.
 */
public class MySqlDialect extends DbDialect {

    @Override
    public String getUpsertQuery(final String table, final List<String> nonKeyColumns, final List<String> keyColumns) {
        if (table == null || table.trim().length() == 0)
            throw new IllegalArgumentException("<table=> is not valid. A non null non empty string expected");

        if (keyColumns == null || keyColumns.size() == 0) {
            throw new IllegalArgumentException("<keyColumns> is invalid. Need to be non null, non empty and be a subset of <columns>");
        }
        //MySql doesn't support SQL 2003:merge so here how the upsert is handled
        final String queryColumns = Joiner.on(",").join(Iterables.concat(nonKeyColumns, keyColumns));
        final String bindingValues = Joiner.on(",").join(Collections.nCopies(nonKeyColumns.size() + keyColumns.size(), "?"));

        final StringBuilder builder = new StringBuilder();
        builder.append(String.format("%s=values(%s)", nonKeyColumns.get(0), nonKeyColumns.get(0)));
        for (int i = 1; i < nonKeyColumns.size(); ++i) {
            builder.append(String.format(",%s=values(%s)", nonKeyColumns.get(i), nonKeyColumns.get(i)));
        }

        final String query = String.format("insert into %s(%s) values(%s) " +
                "on duplicate key update %s", table, queryColumns, bindingValues, builder.toString());
        return query;
    }
}
