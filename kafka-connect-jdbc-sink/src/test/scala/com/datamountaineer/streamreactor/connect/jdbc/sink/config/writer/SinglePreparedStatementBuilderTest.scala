package com.datamountaineer.streamreactor.connect.jdbc.sink.config.writer

import java.sql.{Connection, PreparedStatement}

import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.SinglePreparedStatementBuilder
import com.datamountaineer.streamreactor.connect.jdbc.sink.{BooleanPreparedStatementBinder, IntPreparedStatementBinder, PreparedStatementBinder, StructFieldsDataExtractor}
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

class SinglePreparedStatementBuilderTest extends WordSpec with Matchers with MockitoSugar {
  "SinglePreparedStatementBuilder" should {
    "return a PreparedStatement for each record" in {
      val valueExtractor = mock[StructFieldsDataExtractor]
      val dataBinders = Seq[(String, PreparedStatementBinder)](
        "colA" -> BooleanPreparedStatementBinder(true),
        "colB" -> IntPreparedStatementBinder(3))


      when(valueExtractor.get(any[Struct])).thenReturn(dataBinders)
      val builder = SinglePreparedStatementBuilder("tableA", valueExtractor)

      val schema = SchemaBuilder.struct.name("record")
        .version(1)
        .field("id", Schema.STRING_SCHEMA)
        .field("int_field", Schema.INT32_SCHEMA)
        .field("long_field", Schema.INT64_SCHEMA)
        .field("string_field", Schema.STRING_SCHEMA)
        .build

      val record = new Struct(schema)
      val records = (1 to 10).map(v => new SinkRecord("aa", 1, null, null, schema, record, v))

      val connection = mock[Connection]
      val sql="INSERT INTO tableA(colA,colB) VALUES(?,?)"

      /*val statements = (1 to 10).map(v=>mock[PreparedStatement]).toArray
      when(connection.prepareStatement(sql))
        .thenReturn(statements:_*)

      val statements = builder.build(records)(connection)

      statements.size shouldBe 10
      verify(connection, times(10)).prepareStatement(sql)
      */
    }
  }
}
