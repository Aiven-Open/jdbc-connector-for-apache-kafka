package io.confluent.connect.jdbc.sink.metadata;

import org.apache.kafka.connect.data.Schema;

public class SchemaPair {
  public final Schema keySchema;
  public final Schema valueSchema;

  public SchemaPair(Schema keySchema, Schema valueSchema) {
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SchemaPair that = (SchemaPair) o;

    if (keySchema != null ? !keySchema.equals(that.keySchema) : that.keySchema != null) {
      return false;
    }
    return valueSchema != null ? valueSchema.equals(that.valueSchema) : that.valueSchema == null;
  }

  @Override
  public int hashCode() {
    int result = keySchema != null ? keySchema.hashCode() : 0;
    result = 31 * result + (valueSchema != null ? valueSchema.hashCode() : 0);
    return result;
  }
}
