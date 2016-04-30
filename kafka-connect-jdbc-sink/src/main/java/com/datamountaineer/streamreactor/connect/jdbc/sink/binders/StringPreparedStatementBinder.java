package com.datamountaineer.streamreactor.connect.jdbc.sink.binders;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class StringPreparedStatementBinder implements PreparedStatementBinder {
    private final String value;

    public StringPreparedStatementBinder(String value) {
        this.value = value;
    }

    @Override
    public void bind(int index, PreparedStatement statement) throws SQLException {
        statement.setString(index, value);
    }
}
