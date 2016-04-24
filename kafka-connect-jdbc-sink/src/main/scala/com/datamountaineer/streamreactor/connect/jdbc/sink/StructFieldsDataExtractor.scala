package com.datamountaineer.streamreactor.connect.jdbc.sink

import org.apache.kafka.connect.data.{Field, Schema, Struct}

import scala.collection.JavaConversions._

case class StructFieldsDataExtractor(includeAllFields: Boolean, fieldsAliasMap: Map[String, String]) {

  def get(struct: Struct): Seq[(String, PreparedStatementBinder)] = {
    val schema = struct.schema()
    val fields: Seq[Field] = if (includeAllFields) schema.fields()
    else schema.fields().filter(f => fieldsAliasMap.contains(f.name()))

    val fieldsAndValues = fields.flatMap { case field =>
      getFieldValue(field, struct).map(value => fieldsAliasMap.getOrElse(field.name(), field.name()) -> value)
    }
    fieldsAndValues
  }

  private def getFieldValue(field: Field, struct: Struct): Option[PreparedStatementBinder] = {
    Option(struct.get(field)) match {
      case None => None
      case Some(value) =>
        val fieldName = field.name()
        val value = field.schema() match {
          case Schema.BOOLEAN_SCHEMA | Schema.OPTIONAL_BOOLEAN_SCHEMA => BooleanPreparedStatementBinder(struct.getBoolean(fieldName))
          case Schema.BYTES_SCHEMA | Schema.OPTIONAL_BYTES_SCHEMA => BytesPreparedStatementBinder(struct.getBytes(fieldName))
          case Schema.FLOAT32_SCHEMA | Schema.OPTIONAL_FLOAT32_SCHEMA => FloatPreparedStatementBinder(struct.getFloat32(fieldName))
          case Schema.FLOAT64_SCHEMA | Schema.OPTIONAL_FLOAT64_SCHEMA => DoublePreparedStatementBinder(struct.getFloat64(fieldName))
          case Schema.INT8_SCHEMA | Schema.OPTIONAL_INT8_SCHEMA => BytePreparedStatementBinder(struct.getInt8(fieldName))
          case Schema.INT16_SCHEMA | Schema.OPTIONAL_INT16_SCHEMA => ShortPreparedStatementBinder(struct.getInt16(fieldName))
          case Schema.INT32_SCHEMA | Schema.OPTIONAL_INT32_SCHEMA => IntPreparedStatementBinder(struct.getInt32(fieldName))
          case Schema.INT64_SCHEMA | Schema.OPTIONAL_INT64_SCHEMA => LongPreparedStatementBinder(struct.getInt64(fieldName))
          case Schema.STRING_SCHEMA | Schema.OPTIONAL_STRING_SCHEMA => StringPreparedStatementBinder(struct.getString(fieldName))
        }
        Some(value)
    }
  }
}
