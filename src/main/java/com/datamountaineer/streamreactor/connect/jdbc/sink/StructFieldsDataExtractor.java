/**
 * Copyright 2015 Datamountaineer.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.datamountaineer.streamreactor.connect.jdbc.sink;

import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.BasePreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.BooleanPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.BytePreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.BytesPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.DoublePreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.FloatPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.IntPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.LongPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.PreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.ShortPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.StringPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldAlias;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldsMappings;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * This class holds the a mappings of fields to extract from
 * a Connect Struct record.
 * <p>
 * Used to building mappings fro Struct records to JDBC binding statements.
 * <p>
 * For example, if the struct contains a string field which is part of the aliasMap .i.e.
 * set in configuration to be write to the target, it will return a StringPreparedStatementBinder
 * (statement.setString(index, value))
 */
public class StructFieldsDataExtractor {
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

  public StructFieldsDataExtractor(final FieldsMappings fieldsMappings) {
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
      fields = Collections2.filter(schema.fields(), new Predicate<Field>() {
        @Override
        public boolean apply(Field input) {
          return fieldsMappings.getMappings().containsKey(input.name()) ||
                  fieldsMappings.areAllFieldsIncluded();
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
    final FieldAlias autoPK = mappings.get(FieldsMappings.CONNECT_AUTO_ID_COLUMN);
    if (autoPK != null) {
      //fake the field and binder
      StringPreparedStatementBinder binder = new StringPreparedStatementBinder(FieldsMappings.CONNECT_AUTO_ID_COLUMN,
              FieldsMappings.generateConnectAutoPKValue(record));
      binder.setPrimaryKey(true);
      binders.add(binder);
    }
    for (final Field field : fields) {
      final BasePreparedStatementBinder binder = getPreparedStatementBinder(field, struct);
      if (binder != null) {
        final FieldAlias fa = mappings.get(field.name());
        if (fa != null && fa.isPrimaryKey()) {
          binder.setPrimaryKey(true);
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
   * @param field  The struct field to get the binder for.
   * @param struct The struct which the field belongs to.
   * @return A PreparedStatementBinder for the field.
   */
  private BasePreparedStatementBinder getPreparedStatementBinder(final Field field, final Struct struct) {
    final Object value = struct.get(field);
    if (value == null)
      return null;


    final String fieldName;
    if (fieldsMappings.getMappings().containsKey(field.name()))
      fieldName = fieldsMappings.getMappings().get(field.name()).getName();
    else
      fieldName = field.name();

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
