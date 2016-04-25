package com.datamountaineer.streamreactor.connect.jdbc.sink.config.writer

import java.sql.{Connection, PreparedStatement}

import com.datamountaineer.streamreactor.connect.jdbc.sink._
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.SinglePreparedStatementBuilder
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
        "colB" -> IntPreparedStatementBinder(3),
        "colC" -> LongPreparedStatementBinder(124566),
        "colD" -> StringPreparedStatementBinder("somevalue"),
        "colE" -> DoublePreparedStatementBinder(-5345.22),
        "colF" -> FloatPreparedStatementBinder(0),
        "colG" -> BytePreparedStatementBinder(-24),
        "colH" -> ShortPreparedStatementBinder(-2345))


      when(valueExtractor.get(any[Struct])).thenReturn(dataBinders)
      val builder = SinglePreparedStatementBuilder("tableA", valueExtractor)

      //schema is not used as we mocked the value extractors
      val schema = SchemaBuilder.struct.name("record")
        .version(1)
        .field("id", Schema.STRING_SCHEMA)
        .build

      val record = new Struct(schema)
      val records = (1 to 10).map(v => new SinkRecord("aa", 1, null, null, schema, record, v))

      val connection = mock[Connection]
      val sql = "INSERT INTO tableA(colA,colB,colC,colD,colE,colF,colG,colH) VALUES(?,?,?,?,?,?,?,?)"

      val statements = (1 to 10).map(v => mock[PreparedStatement])
      when(connection.prepareStatement(sql)).thenReturn(statements.head, statements.tail: _*)

      val actualStatements = builder.build(records)(connection)

      actualStatements.size shouldBe 10
      verify(connection, times(10)).prepareStatement(sql)

      statements.foreach { s =>
        verify(s, times(1)).setBoolean(1, true)
        verify(s, times(1)).setInt(2, 3)
        verify(s, times(1)).setLong(3, 124566)
        verify(s, times(1)).setString(4, "somevalue")
        verify(s, times(1)).setDouble(5, -5345.22)
        verify(s, times(1)).setFloat(6, 0)
        verify(s, times(1)).setByte(7, -24)
        verify(s, times(1)).setShort(8, -2345)
      }

    }
  }
}
