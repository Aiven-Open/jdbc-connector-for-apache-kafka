package com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect;

import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class SqLiteDialectTest {
    final DbDialect dialect = new SQLiteDialect();

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfTableIsNull() {
        dialect.getUpsertQuery(null, Lists.newArrayList("value"), Lists.<String>newArrayList("id"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwAnExceptionIfTableNameIsEmptyString() {
        dialect.getUpsertQuery("  ", Lists.newArrayList("value"), Lists.<String>newArrayList("id"));
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
        String insert = dialect.getUpsertQuery("Customer", Lists.newArrayList("name", "salary", "address"), Lists.<String>newArrayList("id"));
        assertEquals(insert, "update or ignore Customer set name=?,salary=?,address=? where id=?\n" +
                        "insert or ignore into Customer(name,salary,address,id) values (?,?,?,?)");

    }

    @Test
    public void produceTheRightSqlStatementWhithACompositePK() {
        String insert = dialect.getUpsertQuery("Book", Lists.newArrayList("ISBN", "year", "pages"), Lists.<String>newArrayList("author", "title"));
        assertEquals(insert, "update or ignore Book set ISBN=?,year=?,pages=? where author=? and title=?\n" +
                "insert or ignore into Book(ISBN,year,pages,author,title) values (?,?,?,?,?)");

    }
}
