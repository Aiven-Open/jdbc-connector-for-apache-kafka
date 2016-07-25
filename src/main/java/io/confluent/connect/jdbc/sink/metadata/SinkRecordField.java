package io.confluent.connect.jdbc.sink.metadata;

import org.apache.kafka.connect.data.Schema;

public class SinkRecordField {
  public final Schema.Type type;
  public final String name;
  public final boolean isPrimaryKey;
  public final boolean isOptional;

  public SinkRecordField(final Schema.Type type, final String name, final boolean isPrimaryKey) {
    this(type, name, isPrimaryKey, !isPrimaryKey);
  }

  public SinkRecordField(
      final Schema.Type type,
      final String name,
      final boolean isPrimaryKey,
      final boolean isOptional
  ) {
    this.type = type;
    this.name = name;
    this.isPrimaryKey = isPrimaryKey;
    this.isOptional = isOptional;
  }

  @Override
  public String toString() {
    return "SinkRecordField{" +
           "type=" + type +
           ", name='" + name + '\'' +
           ", isPrimaryKey=" + isPrimaryKey +
           ", isOptional=" + isOptional +
           '}';
  }
}
