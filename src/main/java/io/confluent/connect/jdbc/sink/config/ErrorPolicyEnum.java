package io.confluent.connect.jdbc.sink.config;

/**
 * Specifies the approach taken when an error occurs while the data is inserted.
 */
public enum ErrorPolicyEnum {
  /**
   * The exception is swallowed.
   */
  NOOP("noop"),
  /**
   * The exception is propagate up the stack by rethrowing it.
   */
  THROW("throw"),

  /**
   * The exception causes the Connect framework to retry the message
   */
  RETRY("retry");

  private final String value;

  ErrorPolicyEnum(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

}
