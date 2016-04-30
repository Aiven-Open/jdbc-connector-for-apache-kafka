package com.datamountaineer.streamreactor.connect.jdbc.sink.binders;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class BytePreparedStatementBinder implements PreparedStatementBinder {
    private final byte value;

    public BytePreparedStatementBinder(byte value) {
        this.value = value;
    }

    @Override
    public void bind(int index, PreparedStatement statement) throws SQLException {
        statement.setByte(index, value);
    }
}
