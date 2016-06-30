package io.confluent.connect.jdbc.sink.writer;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import io.confluent.connect.jdbc.sink.dialect.MySqlDialect;

import static org.junit.Assert.assertEquals;

public class InsertTest {
  @Test(expected = IllegalArgumentException.class)
  public void throwAnErrorIfTheMapIsEmpty() {
    new MySqlDialect().getInsert("sometable", Collections.<String>emptyList(), Collections.<String>emptyList());
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwAnExceptionIfTableNameIsEmpty() {
    new MySqlDialect().getInsert("  ", Collections.singletonList("a"), Collections.<String>emptyList());
  }

  @Test
  public void buildTheCorrectSql() {
    String query = new MySqlDialect().getInsert("customers",
                                                Arrays.asList("age", "firstName", "lastName"),
                                                Collections.<String>emptyList());

    assertEquals(query, "INSERT INTO `customers`(`age`,`firstName`,`lastName`) VALUES(?,?,?)");
  }
}
