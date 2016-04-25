package com.datamountaineer.streamreactor.connect.jdbc.sink.config.writer

import java.sql.{Connection, PreparedStatement}

import com.datamountaineer.streamreactor.connect.jdbc.sink._
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.BatchedPreparedStatementBuilder
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

class BatchedPreparedStatementBuilderTest extends WordSpec with Matchers with MockitoSugar {
  "BatchedPrepareStatementBuilder" should {
    "group all records with the same columns and create a single sql statement" in {
      val valueExtractor = mock[StructFieldsDataExtractor]
      val dataBinders1 = Seq[(String, PreparedStatementBinder)](
        "colA" -> BooleanPreparedStatementBinder(true),
        "colB" -> IntPreparedStatementBinder(3),
        "colC" -> LongPreparedStatementBinder(124566),
        "colD" -> StringPreparedStatementBinder("somevalue"))

      val dataBinders2 = Seq[(String, PreparedStatementBinder)](
        "colE" -> DoublePreparedStatementBinder(-5345.22),
        "colF" -> FloatPreparedStatementBinder(0),
        "colG" -> BytePreparedStatementBinder(-24),
        "colH" -> ShortPreparedStatementBinder(-2345))

      val dataBinders3 = Seq[(String, PreparedStatementBinder)](
        "A" -> IntPreparedStatementBinder(1),
        "B" -> StringPreparedStatementBinder("bishbash")
      )

      when(valueExtractor.get(any[Struct])).thenReturn(dataBinders1, dataBinders2, dataBinders1, dataBinders1, dataBinders3)

      val builder = BatchedPreparedStatementBuilder("tableA", valueExtractor)

      //schema is not used as we mocked the value extractors
      val schema = SchemaBuilder.struct.name("record")
        .version(1)
        .field("id", Schema.STRING_SCHEMA)
        .build


      val record = new Struct(schema)

      //same size as the valueextractor.get returns
      val records = (1 to 5).map(v => new SinkRecord("aa", 1, null, null, schema, record, v))

      val connection = mock[Connection]

      val sql1 = "INSERT INTO tableA(colA,colB,colC,colD) VALUES(?,?,?,?)"
      val statement1 = mock[PreparedStatement]
      when(connection.prepareStatement(sql1)).thenReturn(statement1)

      val sql2 = "INSERT INTO tableA(colE,colF,colG,colH) VALUES(?,?,?,?)"
      val statement2 = mock[PreparedStatement]
      when(connection.prepareStatement(sql2)).thenReturn(statement2)


      val sql3 = "INSERT INTO tableA(A,B) VALUES(?,?)"
      val statement3 = mock[PreparedStatement]
      when(connection.prepareStatement(sql3)).thenReturn(statement3)

      val actualStatements = builder.build(records)(connection)

      actualStatements.size shouldBe 3

      verify(connection, times(1)).prepareStatement(sql1)
      verify(connection, times(1)).prepareStatement(sql2)
      verify(connection, times(1)).prepareStatement(sql3)

      verify(statement1, times(3)).setBoolean(1, true)
      verify(statement1, times(3)).setInt(2, 3)
      verify(statement1, times(3)).setLong(3, 124566)
      verify(statement1, times(3)).setString(4, "somevalue")
      verify(statement1, times(3)).addBatch()

      verify(statement2, times(1)).setDouble(1, -5345.22)
      verify(statement2, times(1)).setFloat(2, 0)
      verify(statement2, times(1)).setByte(3, -24)
      verify(statement2, times(1)).setShort(4, -2345)
      verify(statement2, times(1)).addBatch()

      verify(statement3, times(1)).setInt(1, 1)
      verify(statement3, times(1)).setString(2, "bishbash")
      verify(statement3, times(1)).addBatch()
    }
  }
}