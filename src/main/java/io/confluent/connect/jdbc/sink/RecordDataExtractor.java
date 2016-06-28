package io.confluent.connect.jdbc.sink;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import io.confluent.connect.jdbc.sink.binders.BasePreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.BooleanPreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.BytePreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.BytesPreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.DoublePreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.FloatPreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.IntPreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.LongPreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.PreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.ShortPreparedStatementBinder;
import io.confluent.connect.jdbc.sink.binders.StringPreparedStatementBinder;
import io.confluent.connect.jdbc.sink.config.FieldAlias;
import io.confluent.connect.jdbc.sink.config.FieldsMappings;

/**
 * This class holds the field mappings to extract from the Connect record.
 * <p>
 * Used to building mappings fro Struct records to JDBC binding statements.
 * <p>
 * For example, if the struct contains a string field which is part of the aliasMap .i.e.
 * set in configuration to be write to the target, it will return a StringPreparedStatementBinder
 * (statement.setString(index, value))
 */
public class RecordDataExtractor {
  private final static Logger logger = LoggerFactory.getLogger(RecordDataExtractor.class);
  private final static Comparator<PreparedStatementBinder> sorter = new Comparator<PreparedStatementBinder>() {
    @Override
    public int compare(PreparedStatementBinder left, PreparedStatementBinder right) {
      if (left.isPrimaryKey() == right.isPrimaryKey()) {
        return left.getFieldName().compareTo(right.getFieldName());
      }
      if (!left.isPrimaryKey()) {
        return -1;
      }
      return 1;
    }
  };

  private final FieldsMappings fieldsMappings;

  public RecordDataExtractor(final FieldsMappings fieldsMappings) {
    this.fieldsMappings = fieldsMappings;
  }

  public String getTableName() {
    return fieldsMappings.getTableName();
  }

  public List<PreparedStatementBinder> get(final Struct struct, final SinkRecord record) {
    final Schema schema = struct.schema();
    final Collection<Field> fields;
    if (fieldsMappings.areAllFieldsIncluded()) {
      fields = schema.fields();
    } else {

      final Map<String, FieldAlias> mappings = fieldsMappings.getMappings();
      fields = Collections2.filter(schema.fields(), new Predicate<Field>() {
        @Override
        public boolean apply(Field input) {
          return fieldsMappings.areAllFieldsIncluded() ||
                 mappings.containsKey(input.name());
        }

        @Override
        public boolean equals(Object object) {
          return false;
        }

        public int hashCode() {
          return 0;
        }
      });
    }

    final List<PreparedStatementBinder> binders = new ArrayList<>(fields.size() + 1);
    final Map<String, FieldAlias> mappings = fieldsMappings.getMappings();
    //check for the autocreated PK column
    final FieldAlias connectTopicPK = mappings.get(FieldsMappings.CONNECT_TOPIC_COLUMN);
    if (connectTopicPK != null) {
      //fake the field and binder
      final StringPreparedStatementBinder binderTopic = new StringPreparedStatementBinder(
          FieldsMappings.CONNECT_TOPIC_COLUMN,
          record.topic());
      binderTopic.setPrimaryKey(true);
      binders.add(binderTopic);

      final IntPreparedStatementBinder binderPartition = new IntPreparedStatementBinder(
          FieldsMappings.CONNECT_PARTITION_COLUMN,
          record.kafkaPartition()
      );
      binderPartition.setPrimaryKey(true);
      binders.add(binderPartition);

      final LongPreparedStatementBinder binderOffset = new LongPreparedStatementBinder(
          FieldsMappings.CONNECT_OFFSET_COLUMN,
          record.kafkaOffset()
      );
      binderOffset.setPrimaryKey(true);
      binders.add(binderOffset);
    }
    for (final Field field : fields) {
      final BasePreparedStatementBinder binder = getPreparedStatementBinder(field, struct);
      if (binder != null) {
        final FieldAlias fa = mappings.get(field.name());
        if (fa != null) {
          if (fa.isPrimaryKey()) {
            binder.setPrimaryKey(true);
          }
        }
        binders.add(binder);
      }
    }

    Collections.sort(binders, sorter);
    return binders;
  }

  /**
   * Return a PreparedStatementBinder for a struct fields.
   *
   * @param field The struct field to get the binder for.
   * @param struct The struct which the field belongs to.
   * @return A PreparedStatementBinder for the field.
   */
  private BasePreparedStatementBinder getPreparedStatementBinder(final Field field, final Struct struct) {
    final Object value = struct.get(field);
    if (value == null) {
      return null;
    }

    final String fieldName;
    if (fieldsMappings.getMappings().containsKey(field.name())) {
      fieldName = fieldsMappings.getMappings().get(field.name()).getName();
    } else {
      fieldName = field.name();
    }

    //match on fields schema type to find the correct casting.
    BasePreparedStatementBinder binder;
    switch (field.schema().type()) {
      case INT8:
        binder = new BytePreparedStatementBinder(fieldName, struct.getInt8(field.name()));
        break;
      case INT16:
        binder = new ShortPreparedStatementBinder(fieldName, struct.getInt16(field.name()));
        break;
      case INT32:
        binder = new IntPreparedStatementBinder(fieldName, struct.getInt32(field.name()));
        break;
      case INT64:
        binder = new LongPreparedStatementBinder(fieldName, struct.getInt64(field.name()));
        break;
      case FLOAT32:
        binder = new FloatPreparedStatementBinder(fieldName, struct.getFloat32(field.name()));
        break;
      case FLOAT64:
        binder = new DoublePreparedStatementBinder(fieldName, struct.getFloat64(field.name()));
        break;
      case BOOLEAN:
        binder = new BooleanPreparedStatementBinder(fieldName, struct.getBoolean(field.name()));
        break;
      case STRING:
        binder = new StringPreparedStatementBinder(fieldName, struct.getString(field.name()));
        break;
      case BYTES:
        binder = new BytesPreparedStatementBinder(fieldName, struct.getBytes(field.name()));
        break;
      default:
        throw new IllegalArgumentException("Following schema type " + struct.schema().type() + " is not supported");
    }
    return binder;
  }
}
