package com.datamountaineer.streamreactor.connect.jdbc;

/**
 * Utility class to provide the try catch around a Closeable.close.
 */
public class AutoCloseableHelper {
  public static void close(final AutoCloseable closeable) {
    try {
      closeable.close();
    } catch (Exception e) {

    }
  }
}
