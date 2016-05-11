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

import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.LongPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.BooleanPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.PreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.ShortPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.StringPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.IntPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.BytePreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.FloatPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.DoublePreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.binders.BytesPreparedStatementBinder;
import com.datamountaineer.streamreactor.connect.jdbc.sink.common.FieldAlias;
import com.datamountaineer.streamreactor.connect.jdbc.sink.config.FieldsMappings;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;

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
      return left.getFieldName().compareTo(right.getFieldName());
    }
  };

  private final FieldsMappings fieldsMappings;

  public StructFieldsDataExtractor(final FieldsMappings fieldsMappings) {
    this.fieldsMappings = fieldsMappings;
  }

  public String getTableName() {
    return fieldsMappings.getTableName();
  }


  public PreparedStatementBinders get(final Struct struct) {
    final Schema schema = struct.schema();
    final Collection<Field> fields;
    if (fieldsMappings.areAllFieldsIncluded())
      fields = schema.fields();
    else {
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

    final List<PreparedStatementBinder> nonPrimaryKeyBinders = Lists.newLinkedList();
    final List<PreparedStatementBinder> primaryKeyBinders = Lists.newLinkedList();
    for (final Field field : fields) {
      final PreparedStatementBinder binder = getFieldValue(field, struct);
      if (binder != null) {
        boolean isPk = false;
        if (fieldsMappings.getMappings().containsKey(field.name())) {
          final FieldAlias fa = fieldsMappings.getMappings().get(field.name());
          isPk = fa.isPrimaryKey();
        }

        //final Pair<String, PreparedStatementBinder> pair = new Pair<>(fieldName, binder);
        if (isPk)
          primaryKeyBinders.add(binder);
        else
          nonPrimaryKeyBinders.add(binder);
      }
    }

    nonPrimaryKeyBinders.sort(sorter);

    primaryKeyBinders.sort(sorter);

    return new PreparedStatementBinders(nonPrimaryKeyBinders, primaryKeyBinders);
  }

  /**
   * Return a PreparedStatementBinder for a struct fields.
   *
   * @param field  The struct field to get the binder for.
   * @param struct The struct which the field belongs to.
   * @return A PreparedStatementBinder for the field.
   */
  private PreparedStatementBinder getFieldValue(final Field field, final Struct struct) {
    final Object value = struct.get(field);
    if (value == null)
      return null;


    final String fieldName;
    if (fieldsMappings.getMappings().containsKey(field.name()))
      fieldName = fieldsMappings.getMappings().get(field.name()).getName();
    else
      fieldName = field.name();

    //match on fields schema type to find the correct casting.
    PreparedStatementBinder binder;
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

  public static class PreparedStatementBinders {
    private final List<PreparedStatementBinder> nonKeyColumns;
    private final List<PreparedStatementBinder> keyColumns;

    public PreparedStatementBinders(List<PreparedStatementBinder> nonKeyColumns, List<PreparedStatementBinder> keyColumns) {
      this.nonKeyColumns = nonKeyColumns;
      this.keyColumns = keyColumns;
    }

    public List<PreparedStatementBinder> getNonKeyColumns() {
      return nonKeyColumns;
    }

    public List<PreparedStatementBinder> getKeyColumns() {
      return keyColumns;
    }

    public boolean isEmpty() {
      return nonKeyColumns.isEmpty() && keyColumns.isEmpty();
    }
  }
}

