package com.datamountaineer.streamreactor.connect.config

import io.confluent.common.config.ConfigException


/**
  * Contains the SinkConnect payload fields to consider and/or their mappings
  *
  * @param includeAllFields
  * @param fieldsMappings
  */
case class PayloadFields(includeAllFields: Boolean,
                         fieldsMappings: Map[String, String])


object PayloadFields {
  /**
    * Works out the fields and their mappings to be used when inserting a new Hbase row
    *
    * @param setting - The configuration specifing the fields and their mappings
    * @return A dictionary of fields and their mappings alongside a flag specifying if all fields should be used. If no mapping has been specified the field name is considered to be the mapping
    */
  def apply(setting: Option[String]): PayloadFields = {
    setting match {
      case None => PayloadFields(true, Map.empty[String, String])
      case Some(c) =>

        val mappings = c.split(",").map { case f =>
          f.trim.split("=").toSeq match {
            case Seq(field) =>
              field -> field
            case Seq(field, alias) =>
              field -> alias
            case _ => throw new ConfigException(s"$c is not valid. Need to set the fields and mappings like: field1,field2,field3=alias3,[field4, field5=alias5]")
          }
        }.toMap

        PayloadFields(mappings.contains("*"), mappings - "*")
    }
  }
}
