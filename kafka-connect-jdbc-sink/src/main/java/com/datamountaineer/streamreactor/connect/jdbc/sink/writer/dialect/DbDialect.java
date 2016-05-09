package com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect;

import com.google.common.base.Joiner;

import java.util.Collections;
import java.util.List;

/**
 * Describes which SQL dialect to use. Different databases support different syntax for upserts.
 */
public abstract class DbDialect {
    /**
     * Gets the query allowing to insert a new row into the RDBMS even if it does previously exists
     *
     * @param table
     * @param columns
     * @return
     */
    public abstract String getUpsertQuery(final String table,
                                          final List<String> columns,
                                          final List<String> keyColumns);


    /**
     * Maps a SQL dialect type to an instance of a derived class of DbDialect
     * @param type - The sql dialect type value
     * @return - An instance of DbDialect
     */
    public static DbDialect fromDialectType(DbDialectTypeEnum type) {
        switch (type) {
            case MSSQL:
            case ORACLE:
                return new Sql2003Dialect();

            case SQLITE:
                return new SQLiteDialect();

            case MYSQL:
                return new MySqlDialect();

            default:
                throw new IllegalArgumentException(type + " is not handled");
        }
    }
}