package io.confluent.connect.jdbc.sink.common;

/**
 * Helper class for validating function arguments
 */
public class ParameterValidator {
  /**
   * Checks the given value is not null or empty. If the value doesn't meet the criteria an IllegalArgumentException is
   * thrown
   *
   * @param value - The value to be checked
   * @param paramName - The parameter name of the caller function
   */
  public static void notNullOrEmpty(final String value, final String paramName) {
    if (value == null || value.trim().length() == 0) {
      throw new IllegalArgumentException(String.format("%s is not set correctly.", paramName));
    }
  }

  /**
   * Checks the give obj is not null. If the value provided is null an IllegalArgumentException is thrown.
   */
  public static void notNull(final Object obj, final String paramName) {
    if (obj == null) {
      throw new IllegalArgumentException(String.format("%s is not set correctly.", paramName));
    }
  }
}
