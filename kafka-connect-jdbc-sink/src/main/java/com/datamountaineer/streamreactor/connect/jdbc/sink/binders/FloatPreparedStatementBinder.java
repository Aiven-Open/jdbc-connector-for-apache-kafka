package com.datamountaineer.streamreactor.connect.jdbc.sink.binders;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class FloatPreparedStatementBinder implements PreparedStatementBinder {
    private final float value;

    public FloatPreparedStatementBinder(float value) {
        this.value = value;
    }

    @Override
    public void bind(int index, PreparedStatement statement) throws SQLException {
        statement.setFloat(index, value);
    }

    public float getValue(){
        return value;
    }
}
