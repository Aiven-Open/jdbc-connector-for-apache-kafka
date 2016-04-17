package com.datamountaineer.streamreactor.connect

import org.apache.kafka.connect.data.{Field, Schema, Struct}

import scala.collection.JavaConversions._

trait StructFieldsValuesExtractor {
  def get(struct: Struct): Seq[(String, Any)]
}

case class StructFieldsExtractor(includeAllFields: Boolean, fieldsAliasMap: Map[String, String]) extends StructFieldsValuesExtractor {

  def get(struct: Struct): Seq[(String, AnyRef)] = {
    val schema = struct.schema()
    val fields: Seq[Field] = if (includeAllFields) schema.fields()
    else schema.fields().filter(f => fieldsAliasMap.contains(f.name()))

    val fieldsAndValues = fields.flatMap { case field =>
      getFieldValue(field, struct).map(value => fieldsAliasMap.getOrElse(field.name(), field.name()) -> value)
    }
    fieldsAndValues
  }

  private def getFieldValue(field: Field, struct: Struct): Option[AnyRef] = {
    Option(struct.get(field)) match {
      case None => None
      case Some(value) =>
        val fieldName = field.name()
        val value = field.schema() match {
          case Schema.BOOLEAN_SCHEMA | Schema.OPTIONAL_BOOLEAN_SCHEMA => struct.getBoolean(fieldName)
          case Schema.BYTES_SCHEMA | Schema.OPTIONAL_BYTES_SCHEMA => struct.getBytes(fieldName)
          case Schema.FLOAT32_SCHEMA | Schema.OPTIONAL_FLOAT32_SCHEMA => struct.getFloat32(fieldName)
          case Schema.FLOAT64_SCHEMA | Schema.OPTIONAL_FLOAT64_SCHEMA => struct.getFloat64(fieldName)
          case Schema.INT8_SCHEMA | Schema.OPTIONAL_INT8_SCHEMA => struct.getInt8(fieldName)
          case Schema.INT16_SCHEMA | Schema.OPTIONAL_INT16_SCHEMA => struct.getInt16(fieldName)
          case Schema.INT32_SCHEMA | Schema.OPTIONAL_INT32_SCHEMA => struct.getInt32(fieldName)
          case Schema.INT64_SCHEMA | Schema.OPTIONAL_INT64_SCHEMA => struct.getInt64(fieldName)
          case Schema.STRING_SCHEMA | Schema.OPTIONAL_STRING_SCHEMA => struct.getString(fieldName)
        }
        Some(value)
    }
  }
}
