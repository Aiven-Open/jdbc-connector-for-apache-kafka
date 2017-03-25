package io.confluent.connect.jdbc.util;

import org.apache.kafka.connect.errors.ConnectException;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.easymock.EasyMock.anyObject;
import static org.junit.Assert.assertNotNull;

@RunWith(PowerMockRunner.class)
@PrepareForTest({CachedConnectionProvider.class, DriverManager.class})
@PowerMockIgnore("javax.management.*")
public class CachedConnectionProviderTest {


  @Before
  public void setup(){
    PowerMock.mockStatic(DriverManager.class);
  }

  @Test
  public void retryTillFailure() throws SQLException {
    int retries = 15;

    CachedConnectionProvider connectionProvider = new CachedConnectionProvider("url", "user", "password",
    retries, 100L);

    EasyMock.expect(DriverManager.getConnection(anyObject(String.class), anyObject(String.class), anyObject(String.class)))
      .andThrow(new SQLException()).times(retries);

    PowerMock.replayAll();

    try {
      connectionProvider.getValidConnection();
    }catch(ConnectException ce){
      assertNotNull(ce);
    }

    PowerMock.verifyAll();
  }


@Test
  public void retryTillConnect() throws SQLException {

    Connection connection = EasyMock.createMock(Connection.class);
    int retries = 15;

    CachedConnectionProvider connectionProvider = new CachedConnectionProvider("url", "user", "password",
      retries, 100L);

    EasyMock.expect(DriverManager.getConnection(anyObject(String.class), anyObject(String.class), anyObject(String.class)))
      .andThrow(new SQLException()).times(retries-1).andReturn(connection);

    PowerMock.replayAll();

    assertNotNull(connectionProvider.getValidConnection());

    PowerMock.verifyAll();
  }

}
