package com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect;

import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MySqlDialectTest {
    private final DbDialect dialect = new MySqlDialect();
    
    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfTableIsNull() {
        dialect.getUpsertQuery(null, Lists.newArrayList("value"), Lists.newArrayList("id"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfTableNameIsEmptyString() {
        dialect.getUpsertQuery("  ", Lists.newArrayList("value"), Lists.newArrayList("id"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfKeyColsIsNull() {
        dialect.getUpsertQuery("Person", Lists.newArrayList("value"), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfKeyColsIsNullIsEmpty() {
        dialect.getUpsertQuery("Customer", Lists.newArrayList("value"), Lists.<String>newArrayList());
    }

    @Test
    public void produceTheRightSqlStatementWhithASinglePK() {
        String insert = dialect.getUpsertQuery("Customer", Lists.newArrayList("name", "salary", "address"), Lists.newArrayList("id"));
        assertEquals(insert, "insert into Customer(name,salary,address,id) values(?,?,?,?) " +
                "on duplicate key update name=values(name),salary=values(salary),address=values(address)");

    }

    @Test
    public void produceTheRightSqlStatementWhithACompositePK() {
        String insert = dialect.getUpsertQuery("Book", Lists.newArrayList("ISBN", "year", "pages"), Lists.newArrayList("author", "title"));
        assertEquals(insert, "insert into Book(ISBN,year,pages,author,title) values(?,?,?,?,?) " +
                "on duplicate key update ISBN=values(ISBN),year=values(year),pages=values(pages)");

    }
}
