package com.datamountaineer.streamreactor.connect.jdbc.sink.binders;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class ShortPreparedStatementBinder implements PreparedStatementBinder {
    private final short value;

    public ShortPreparedStatementBinder(short value) {
        this.value = value;
    }

    @Override
    public void bind(int index, PreparedStatement statement) throws SQLException {
        statement.setShort(index, value);
    }

    public short getValue(){
        return value;
    }
}
