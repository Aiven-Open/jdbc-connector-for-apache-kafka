package com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

import java.util.List;

public class Sql2003Dialect extends DbDialect {

    @Override
    public String getUpsertQuery(String table, List<String> columns, List<String> keyColumns) {
        if (table == null || table.trim().length() == 0)
            throw new IllegalArgumentException("<table> is not valid");

        if (keyColumns == null || keyColumns.size() == 0) {
            throw new IllegalArgumentException("<keyColumns> is not valid. It has to be non null and non empty.");
        }
        final String select = Joiner.on(", ? ").join(Iterables.concat(columns, keyColumns));
        final StringBuilder joinBuilder = new StringBuilder();
        joinBuilder.append(String.format("%s.%s=incoming.%s", table, keyColumns.get(0), keyColumns.get(0)));
        for (int i = 1; i < keyColumns.size(); ++i) {
            joinBuilder.append(String.format(" and %s.%s=incoming.%s", table, keyColumns.get(i), keyColumns.get(i)));
        }

        String updateSet = null;
        if (columns.size() > 0) {
            final StringBuilder updateSetBuilder = new StringBuilder("when matched then update set ");
            updateSetBuilder.append(String.format("%s.%s=incoming.%s", table, columns.get(0), columns.get(0)));
            for (int i = 1; i < columns.size(); ++i) {
                updateSetBuilder.append(String.format(",%s.%s=incoming.%s", table, columns.get(i), columns.get(i)));
            }
            updateSet = updateSetBuilder.toString();
        }

        final String insertColumns = Joiner.on(String.format(",%s.", table)).join(Iterables.concat(columns, keyColumns));
        final String insertValues = Joiner.on(",incoming.").join(Iterables.concat(columns, keyColumns));

        /*
        https://blogs.oracle.com/cmar/entry/using_merge_to_do_an"
         */
        return "merge into " + table +
                " using (select ? " + select + ") incoming" +
                " on(" + joinBuilder.toString() + ") " +
                updateSet +
                String.format(" when not matched then insert(%s.%s) values(incoming.%s)",
                        table,
                        insertColumns,
                        insertValues);

    }
}
