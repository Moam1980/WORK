/*
 * TODO: License goes here!
 */

package sa.com.mobily.parsing

import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

class RowParserTest extends FlatSpec with ShouldMatchers {

  trait WithRowData {

    case class Cell(cellId: Long)

    implicit val rowParser = new RowParser[Cell] {

      override def fromRow(row: Row): Cell = {
        val Array(lac, sac) = row.toSeq.toArray
        Cell((lac.asInstanceOf[String] ++ sac.asInstanceOf[String]).toLong)
      }
    }
    val row1 = Row("1111", "2222")
    val row2 = Row("1111", "3333")
    val wrongRow = Row("1111", "NaN")
    val rows = Seq(row1, row2)
    val rowsWrong = Seq(row1, row2, wrongRow)
  }

  "RowParser" should  "parse correct rows" in new WithRowData {
    rowParser.fromRow(row1) should be (Cell(11112222L))
  }

  it should "throw an exception when parsing incorrect rows" in new WithRowData {
    an[Exception] should be thrownBy rowParser.fromRow(wrongRow)
  }

  it should "iterate over entries" in new WithRowData {
    RowParser.fromRows(rows.iterator).length should be (2)
  }

  it should "throw an exception when parsing a list with incorrect rows" in new WithRowData {
    an[Exception] should be thrownBy RowParser.fromRows(rowsWrong.iterator).length
  }
}
