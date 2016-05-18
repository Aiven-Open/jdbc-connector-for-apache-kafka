package com.datamountaineer.streamreactor.connect.jdbc.sink;

import org.junit.*;

import java.io.*;
import java.net.*;
import java.nio.charset.*;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by andrew@datamountaineer.com on 17/05/16.
 * kafka-connect-jdbc
 */
public class SchemaRetrieverTest {

  @Test
  public void GetSchemaTest() throws IOException {
    final HttpURLConnection mockCon = mock(HttpURLConnection. class);

    String schema = "{\"name\": \"test\",\"version\": 1,\"schema\": \"{\"type\": \"string\"}}";

    InputStream response = new ByteArrayInputStream(schema.getBytes(StandardCharsets.UTF_8));
    when(mockCon.getResponseMessage()).thenReturn("");
    when(mockCon.getResponseCode()).thenReturn(200);
    when(mockCon.getInputStream()).thenReturn(response);


    //mocking httpconnection by URLStreamHandler since we can not mock URL class.
    URLStreamHandler stubURLStreamHandler = new URLStreamHandler() {
      @Override
      protected URLConnection openConnection(URL u ) throws IOException {
        return mockCon ;
      }
    };


    URL url = new URL(null,"https://www.google.com", stubURLStreamHandler);
    SchemaRetriever retriever = new SchemaRetriever();
    String ret = retriever.getSchema(url);
    assertTrue(schema.equals(ret));
  }

  @Test(expected = RuntimeException.class)
  public void GetSchemaTestFail() throws IOException {
    final HttpURLConnection mockCon = mock(HttpURLConnection. class);

    String schema = "{\"name\": \"test\",\"version\": 1,\"schema\": \"{\"type\": \"string\"}}";

    InputStream response = new ByteArrayInputStream(schema.getBytes(StandardCharsets.UTF_8));
    when(mockCon.getResponseMessage()).thenReturn("");
    when(mockCon.getResponseCode()).thenReturn(404);
    when(mockCon.getInputStream()).thenReturn(response);


    //mocking httpconnection by URLStreamHandler since we can not mock URL class.
    URLStreamHandler stubURLStreamHandler = new URLStreamHandler() {
      @Override
      protected URLConnection openConnection(URL u ) throws IOException {
        return mockCon ;
      }
    };


    URL url = new URL(null,"https://www.google.com", stubURLStreamHandler);
    SchemaRetriever retriever = new SchemaRetriever();
    retriever.getSchema(url);
  }
}
