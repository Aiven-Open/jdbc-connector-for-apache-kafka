package com.datamountaineer.streamreactor.connect.jdbc.sink.config.writer

import java.sql.PreparedStatement

import com.datamountaineer.streamreactor.connect.jdbc.sink._
import com.datamountaineer.streamreactor.connect.jdbc.sink.writer.PreparedStatementBindDataFn
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

class PreparedStatementBindDataFnTest extends WordSpec with Matchers with MockitoSugar {
  "PreparedStatementBindDataFn" should {
    "bind all the given values to the sql statement" in {
      val statement = mock[PreparedStatement]
      val values = Seq(BooleanPreparedStatementBinder(true),
        BytePreparedStatementBinder(8.toByte),
        ShortPreparedStatementBinder(-24),
        IntPreparedStatementBinder(3),
        LongPreparedStatementBinder(612111),
        FloatPreparedStatementBinder(15.12.toFloat),
        DoublePreparedStatementBinder(-235426.6677),
        StringPreparedStatementBinder("some value")
      )
      PreparedStatementBindDataFn(statement, values)

      verify(statement, times(1)).setBoolean(1, true)
      verify(statement, times(1)).setByte(2, 8)
      verify(statement, times(1)).setShort(3, -24)
      verify(statement, times(1)).setInt(4, 3)
      verify(statement, times(1)).setLong(5, 612111)
      verify(statement, times(1)).setFloat(6, 15.12.toFloat)
      verify(statement, times(1)).setDouble(7, -235426.6677)
      verify(statement, times(1)).setString(8, "some value")

    }
  }
}
