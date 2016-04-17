package com.datamountaineer.streamreactor.connect.sink

import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConversions._

/**
  * Builds the new record key for the given connect SinkRecord.
  */
trait StringKeyBuilder {
  def build(record: SinkRecord): String
}

/**
  * Uses the connect record (topic, partition, offset) to set the schema
  *
  * @param keyDelimiter
  */
class StringGenericRowKeyBuilder(keyDelimiter: String = ".") extends StringKeyBuilder {

  override def build(record: SinkRecord): String = {
    Seq(record.topic(), record.kafkaPartition(), record.kafkaOffset().toString).mkString(keyDelimiter)
  }
}

/**
  * Creates a key based on the connect SinkRecord instance key. Only connect Schema primitive types are handled
  */
class SinkRecordKeyStringKeyBuilder extends StringKeyBuilder {
  override def build(record: SinkRecord): String = {
    val `type` = record.keySchema().`type`()
    require(`type`.isPrimitive, "The SinkRecord key schema is not a primitive type")

    `type`.name() match {
      case "INT8" | "INT16" | "INT32" | "INT64" | "FLOAT32" | "FLOAT64" | "BOOLEAN" | "STRING" | "BYTES" => record.key().toString
      case other => throw new IllegalArgumentException(s"$other is not supported by the ${getClass.getName}")
    }

  }
}

/**
  * Builds a new key from the payload fields specified
  *
  * @param keys
  * @param keyDelimiter
  */
case class StructFieldsStringKeyBuilder(val keys: Seq[String],
                                        val keyDelimiter: String = ".") extends StringKeyBuilder {
  private val availableSchemas = Set(
    Schema.BOOLEAN_SCHEMA, Schema.OPTIONAL_BOOLEAN_SCHEMA,
    Schema.BYTES_SCHEMA, Schema.OPTIONAL_BYTES_SCHEMA,
    Schema.FLOAT32_SCHEMA, Schema.OPTIONAL_FLOAT32_SCHEMA,
    Schema.FLOAT64_SCHEMA, Schema.OPTIONAL_FLOAT64_SCHEMA,
    Schema.INT8_SCHEMA, Schema.OPTIONAL_INT8_SCHEMA,
    Schema.INT16_SCHEMA, Schema.OPTIONAL_INT16_SCHEMA,
    Schema.INT32_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA,
    Schema.INT64_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA,
    Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)

  require(keys.nonEmpty, "Invalid keys provided")

  override def build(record: SinkRecord): String = {
    val struct = record.value().asInstanceOf[Struct]
    val schema = struct.schema

    val availableFields = schema.fields().map(_.name).toSet
    val missingKeys = keys.filterNot(availableFields.contains)
    require(missingKeys.isEmpty, s"${missingKeys.mkString(",")} keys are not present in the SinkRecord payload:${availableFields.mkString(",")}")

    keys.flatMap { case key =>
      val field = schema.field(key)
      val value = struct.get(field)
      require(value != null, s"$key field value is null. Non null value is required for the fileds creating the Hbase row key")
      if (availableSchemas.contains(field.schema())) Some(value.toString)
      else None
    }.mkString(keyDelimiter)
  }
}