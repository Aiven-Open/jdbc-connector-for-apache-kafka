package com.datamountaineer.streamreactor.connect.jdbc.sink.avro;

import com.datamountaineer.streamreactor.connect.jdbc.sink.SinkRecordField;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldAlias;
import com.google.common.collect.Lists;
import org.apache.kafka.connect.data.Schema;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by andrew@datamountaineer.com on 22/05/16.
 * kafka-connect-jdbc
 */
public class AvroToDbConverter {


  public Collection<SinkRecordField> convert(String inputSchema, Map<String, FieldAlias> mappings) {
    org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(inputSchema);
    List<org.apache.avro.Schema.Field> fields = schema.getFields();
    List<SinkRecordField> converted = Lists.newArrayList();

    for (org.apache.avro.Schema.Field avro : fields) {
      String fieldName = avro.name();
      if (mappings.containsKey(fieldName)) {
        fieldName = mappings.get(fieldName).getName();
      }
      converted.add(fromAvro(avro.schema(), fieldName));
    }
    return converted;
  }

  public SinkRecordField fromAvro(org.apache.avro.Schema schema, String fieldName) {

    switch (schema.getType()) {
      case RECORD:
        throw new RuntimeException("Avro type RECORD not supported");
      case ARRAY:
        throw new RuntimeException("Avro type ARRAY not supported");
      case MAP:
        throw new RuntimeException("Avro type MAP not supported");
      case UNION:
        org.apache.avro.Schema union = getNonNull(schema);
        return fromAvro(union, fieldName);

      case FIXED:
        return new SinkRecordField(Schema.Type.BYTES, fieldName, false);

      case STRING:
        return new SinkRecordField(Schema.Type.STRING, fieldName, false);

      case BYTES:
        return new SinkRecordField(Schema.Type.BYTES, fieldName, false);

      case INT:
        return new SinkRecordField(Schema.Type.INT32, fieldName, false);

      case LONG:
        return new SinkRecordField(Schema.Type.INT64, fieldName, false);

      case FLOAT:
        return new SinkRecordField(Schema.Type.FLOAT64, fieldName, false);

      case DOUBLE:
        return new SinkRecordField(Schema.Type.FLOAT64, fieldName, false);

      case BOOLEAN:
        return new SinkRecordField(Schema.Type.BOOLEAN, fieldName, false);

      case NULL:
        throw new RuntimeException("Avro type NULL not supported");
      default:
        throw new RuntimeException("Avro type not supported");
    }
  }

  org.apache.avro.Schema getNonNull(org.apache.avro.Schema schema) {
    List<org.apache.avro.Schema> unionTypes = schema.getTypes();
    if (unionTypes.size() == 2) {
      if (unionTypes.get(0).getType().equals(org.apache.avro.Schema.Type.NULL)) {
        return unionTypes.get(1);
      } else if (unionTypes.get(1).getType().equals(org.apache.avro.Schema.Type.NULL)) {
        return unionTypes.get(0);
      } else {
        return schema;
      }
    } else {
      return schema;
    }
  }
}
