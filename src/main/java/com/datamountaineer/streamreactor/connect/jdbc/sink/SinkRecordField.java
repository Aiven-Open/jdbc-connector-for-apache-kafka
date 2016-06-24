package com.datamountaineer.streamreactor.connect.jdbc.sink;

import org.apache.kafka.connect.data.Schema;

/**
 * Contains the SinkRecord field and schema. It is used in conjunction with table schema evolution
 */
public class SinkRecordField {
  private final boolean isPrimaryKey;
  private final Schema.Type type;
  private final String name;

  public SinkRecordField(final Schema.Type type, final String name, final  boolean isPrimaryKey) {
    this.type = type;
    this.name = name;
    this.isPrimaryKey = isPrimaryKey;
  }

  public Schema.Type getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }
}
