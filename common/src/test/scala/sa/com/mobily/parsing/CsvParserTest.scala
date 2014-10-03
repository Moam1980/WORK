/*
 * TODO: License goes here!
 */

package sa.com.mobily.parsing

import org.scalatest.{FlatSpec, ShouldMatchers}

class CsvParserTest extends FlatSpec with ShouldMatchers {

  trait WithCsvData {

    case class Cell(cellId: Long)

    implicit val csvParser = new CsvParser[Cell] {

      def fromFields(fields: Array[String]): Cell = {
        val Array(lac, sac) = fields
        Cell(cellId = (lac ++ sac).toLong)
      }
    }

    val lines = Seq("1111,2222", "", "4234, 1234", "1111,NaN")
  }

  "CsvParser" should "identify wrong formatted fields" in new WithCsvData {
    val parsedItem = CsvParser.fromLine("1111,NaN")
    parsedItem.value should be (None)
    parsedItem.parsingError.get.line should be ("1111,NaN")
    parsedItem.parsingError.get.error shouldBe a [NumberFormatException]
  }

  it should "identify arrays out of bounds" in new WithCsvData {
    val parsedItem = CsvParser.fromLine("")
    parsedItem.value should be (None)
    parsedItem.parsingError.get.line should be ("")
    parsedItem.parsingError.get.error shouldBe a [MatchError]
  }

  it should "parse correct entries" in new WithCsvData {
    val parsedItem = CsvParser.fromLine("1111,2222")
    parsedItem.value.get should be (Cell(11112222L))
    parsedItem.parsingError should be (None)
  }

  it should "iterate over entries" in new WithCsvData {
    CsvParser.fromLines(lines.iterator).length should be (4)
  }
}
