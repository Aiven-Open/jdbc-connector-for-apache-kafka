package com.datamountaineer.streamreactor.connect.jdbc.sink.binders;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class IntPreparedStatementBinder implements PreparedStatementBinder {
    private final int value;

    public IntPreparedStatementBinder(int value) {
        this.value = value;
    }

    @Override
    public void bind(int index, PreparedStatement statement) throws SQLException {
        statement.setInt(index, value);
    }
}
