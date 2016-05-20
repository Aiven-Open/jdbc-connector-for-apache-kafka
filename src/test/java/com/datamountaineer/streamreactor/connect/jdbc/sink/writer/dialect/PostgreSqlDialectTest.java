package com.datamountaineer.streamreactor.connect.jdbc.sink.writer.dialect;

import com.google.common.collect.Lists;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

/**
 * Created by andrew@datamountaineer.com on 17/05/16.
 * kafka-connect-jdbc
 */
public class PostgreSqlDialectTest {

  private final DbDialect dialect = new PostgreSQLDialect();

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
  public void PostgreDialectTest() {
    String conflict = "INSERT INTO Customer (name,salary,address,id) VALUES (?,?,?,?) ON CONFLICT (id) DO UPDATE SET " +
        "name=EXCLUDED.name,salary=EXCLUDED.salary,address=EXCLUDED.address";
    String insert = dialect.getUpsertQuery("Customer", Lists.newArrayList("name", "salary", "address"), Lists.newArrayList("id"));
    assertTrue(conflict.equals(insert));
  }
}
