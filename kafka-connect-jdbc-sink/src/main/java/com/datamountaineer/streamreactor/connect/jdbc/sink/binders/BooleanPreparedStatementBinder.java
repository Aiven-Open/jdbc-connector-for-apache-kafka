package com.datamountaineer.streamreactor.connect.jdbc.sink.binders;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class BooleanPreparedStatementBinder implements PreparedStatementBinder {
    private final boolean value;

    public BooleanPreparedStatementBinder(boolean value) {
        this.value = value;
    }

    @Override
    public void bind(int index, PreparedStatement statement) throws SQLException {
        statement.setBoolean(index, value);
    }


    public boolean getValue(){
        return value;
    }
}
