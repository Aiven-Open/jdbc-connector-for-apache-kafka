package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;

import com.datamountaineer.streamreactor.connect.jdbc.sink.config.JdbcSinkSettings;
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect.DbDialect;

/**
 * Helper class for creating an instance of QueryBuilder
 */
public class QueryBuilderHelper {
    /**
     * Creates an instance of DbDialect from the jdbc sink settings.
     *
     * @param settings - The jdbc sink settings
     * @return - An instance of DbDialect
     */
    public static QueryBuilder from(final JdbcSinkSettings settings) {
        if (settings.getFields().hasPrimaryKeys()) {
            final DbDialect dialect = DbDialect.fromDialectType(settings.getDialectType());
            return new UpsertQueryBuilder(dialect);
        }
        return new InsertQueryBuilder();

    }
}
