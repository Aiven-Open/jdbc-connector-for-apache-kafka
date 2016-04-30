package com.datamountaineer.streamreactor.connect.jdbc.sink.binders;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class DoublePreparedStatementBinder implements PreparedStatementBinder {
    private final double value;

    public DoublePreparedStatementBinder(double value) {
        this.value = value;
    }

    @Override
    public void bind(int index, PreparedStatement statement) throws SQLException {
        statement.setDouble(index, value);
    }
}
