package com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect;

import com.google.common.collect.*;
import org.junit.*;

import static junit.framework.TestCase.assertTrue;

/**
 * Created by andrew@datamountaineer.com on 17/05/16.
 * kafka-connect-jdbc
 */
public class PostgreDialectTest {

  final DbDialect dialect = new PostgreDialect();

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
  public void PostgreDialectTest() {
    String conflict = "INSERT INTO Customer(name,Customer.salary,Customer.address,Customer.id) VALUES " +
    "(name,incoming.salary,incoming.address,incoming.id) ON CONFLICT (id) UPDATE Customer.name=incoming.name," +
    "Customer.salary=incoming.salary,Customer.address=incoming.address";
    String insert = dialect.getUpsertQuery("Customer", Lists.newArrayList("name", "salary", "address"), Lists.newArrayList("id"));
    assertTrue(conflict.equals(insert));
  }
}
