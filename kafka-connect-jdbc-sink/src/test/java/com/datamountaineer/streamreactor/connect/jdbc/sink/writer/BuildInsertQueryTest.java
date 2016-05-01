package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;

import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.BuildInsertQuery;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class BuildInsertQueryTest {
    @Test(expected = IllegalArgumentException.class)
    public void throwAnErrorIfTheMapIsEmpty() {
        BuildInsertQuery.get("sometable", new ArrayList<String>());
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfTableNameIsEmpty() {
        BuildInsertQuery.get("  ", Lists.newArrayList("a"));
    }

    @Test
    public void buildTheCorrectSql() {
        String query = BuildInsertQuery.get("customers",
                Lists.newArrayList("age", "firstName", "lastName"));

        assertEquals(query, "INSERT INTO customers(age,firstName,lastName) VALUES(?,?,?)");
    }
}
