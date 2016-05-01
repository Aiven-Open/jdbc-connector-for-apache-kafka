package com.datamountaineer.streamreactor.connect.jdbc.sink.binders;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class LongPreparedStatementBinder implements PreparedStatementBinder {
    private final long value;

    public LongPreparedStatementBinder(long value) {
        this.value = value;
    }

    @Override
    public void bind(int index, PreparedStatement statement) throws SQLException {
        statement.setLong(index, value);
    }


    public long getValue(){
        return value;
    }
}
