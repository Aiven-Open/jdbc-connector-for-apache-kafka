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

package com.datamountaineer.streamreactor.connect.jdbc.sink;

import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class JdbcDriverLoaderTest {
  @Test(expected = IllegalArgumentException.class)
  public void throwAnExceptionIfTheJdbcJarDoesNotExists() {
    JdbcDriverLoader.load("somedriver", new File("bogus.jar"));
  }

  @Test
  public void loadTheOracleDriver() throws URISyntaxException {
    File jar = Paths.get(getClass().getResource("/ojdbc7.jar").toURI()).toAbsolutePath().toFile();
    String driver = "oracle.jdbc.OracleDriver";
    assertEquals(JdbcDriverLoader.load(driver, jar), true);
    assertEquals(JdbcDriverLoader.load(driver, jar), false);
  }

  @Test
  public void loadTheMSDriver() throws URISyntaxException {
    File jar = Paths.get(getClass().getResource("/sqljdbc4-4-4.0.jar").toURI()).toAbsolutePath().toFile();
    String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    assertEquals(JdbcDriverLoader.load(driver, jar), true);
    assertEquals(JdbcDriverLoader.load(driver, jar), false);
  }
}
