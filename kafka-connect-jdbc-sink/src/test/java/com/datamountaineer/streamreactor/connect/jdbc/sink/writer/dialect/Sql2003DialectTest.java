package com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect;


import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Sql2003DialectTest {
    final DbDialect dialect = new Sql2003Dialect();

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
        String insert = dialect.getUpsertQuery("Customer", Lists.newArrayList("name", "salary", "address"), Lists.newArrayList("id"));
        assertEquals(insert, "merge into Customer using (select ? name, ? salary, ? address, ? id) incoming on(Customer.id=incoming.id) " +
                "when matched then update set Customer.name=incoming.name,Customer.salary=incoming.salary,Customer.address=incoming.address" +
                " when not matched then insert(Customer.name,Customer.salary,Customer.address,Customer.id) values(incoming.name,incoming.salary,incoming.address,incoming.id)");

    }

    @Test
    public void produceTheRightSqlStatementWhithACompositePK() {
        String insert = dialect.getUpsertQuery("Book", Lists.newArrayList("ISBN", "year", "pages"), Lists.newArrayList("author", "title"));
        assertEquals(insert, "merge into Book using (select ? ISBN, ? year, ? pages, ? author, ? title) incoming on(Book.author=incoming.author and Book.title=incoming.title) " +
                "when matched then update set Book.ISBN=incoming.ISBN,Book.year=incoming.year,Book.pages=incoming.pages" +
                " when not matched then insert(Book.ISBN,Book.year,Book.pages,Book.author,Book.title) values(incoming.ISBN,incoming.year,incoming.pages,incoming.author,incoming.title)");

    }
}
