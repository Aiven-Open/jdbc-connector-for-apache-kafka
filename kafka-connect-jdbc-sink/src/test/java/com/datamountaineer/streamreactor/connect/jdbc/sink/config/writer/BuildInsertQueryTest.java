package com.datamountaineer.streamreactor.connect.jdbc.sink.config.writer;

import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.BuildInsertQuery;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class BuildInsertQueryTest {
    @Test(expected = IllegalArgumentException.class)
    public void throwAnErrorIfTheMapIsEmpty() {
        BuildInsertQuery.apply("sometable", new ArrayList<String>());
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfTableNameIsEmpty() {
        BuildInsertQuery.apply("  ", Lists.newArrayList("a"));
    }

    @Test
    public void buildTheCorrectSql() {
        String query = BuildInsertQuery.apply("customers",
                Lists.newArrayList("age", "firstName", "lastName"));

        assertEquals(query, "INSERT INTO customers(age,firstName,lastName) VALUES(?,?,?)");
    }
}
