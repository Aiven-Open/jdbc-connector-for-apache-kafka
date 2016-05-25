package com.datamountaineer.streamreactor.connect.jdbc.sink.writer;

import com.datamountaineer.streamreactor.connect.jdbc.dialect.MySqlDialect;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class InsertTest {
  @Test(expected = IllegalArgumentException.class)
  public void throwAnErrorIfTheMapIsEmpty() {
    new MySqlDialect().getInsert("sometable", Lists.<String>newArrayList(), Lists.<String>newArrayList());
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwAnExceptionIfTableNameIsEmpty() {
    new MySqlDialect().getInsert("  ", Lists.newArrayList("a"), Lists.<String>newArrayList());
  }

  @Test
  public void buildTheCorrectSql() {
    String query = new MySqlDialect().getInsert("customers",
            Lists.newArrayList("age", "firstName", "lastName"),
            Lists.<String>newArrayList());

    assertEquals(query, "INSERT INTO `customers`(`age`,`firstName`,`lastName`) VALUES(?,?,?)");
  }
}
