package com.datamountaineer.streamreactor.connect.jdbc.sink.config;

/**
 * Specifies the approach taken when an error occurs while the data is inserted.
 */
public enum ErrorPolicyEnum {
    NOOP("noop"),
    THROW("throw");

    private final String value;

    private ErrorPolicyEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

}
