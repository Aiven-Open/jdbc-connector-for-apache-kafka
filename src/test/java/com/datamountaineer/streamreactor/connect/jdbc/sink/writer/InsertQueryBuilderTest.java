package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;

import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class InsertQueryBuilderTest {
  @Test(expected = IllegalArgumentException.class)
  public void throwAnErrorIfTheMapIsEmpty() {
    new InsertQueryBuilder().build("sometable", Lists.<String>newArrayList(), Lists.<String>newArrayList());
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwAnExceptionIfTableNameIsEmpty() {
    new InsertQueryBuilder().build("  ", Lists.newArrayList("a"), Lists.<String>newArrayList());
  }

  @Test
  public void buildTheCorrectSql() {
    String query = new InsertQueryBuilder().build("customers",
            Lists.newArrayList("age", "firstName", "lastName"),
            Lists.<String>newArrayList());

    assertEquals(query, "INSERT INTO customers(age,firstName,lastName) VALUES(?,?,?)");
  }
}
