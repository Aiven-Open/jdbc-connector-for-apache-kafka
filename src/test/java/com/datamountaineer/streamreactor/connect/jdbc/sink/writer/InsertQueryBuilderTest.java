/**
 * Copyright 2015 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

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
