/*
 * TODO: License goes here!
 */

package sa.com.mobily.parsing

import org.scalatest.{FlatSpec, ShouldMatchers}

class CsvParserTest extends FlatSpec with ShouldMatchers {

  trait WithParsingError {

    val line = "Test error in a line"
    val errorMessage = "Just a test"
    val error = new Throwable(errorMessage)

    val parsingError = ParsingError(
      line = line,
      error = error)
  }

  trait WithCsvData {

    case class Cell(cellId: Long)

    final val lineCsvParserObject = new OpenCsvParser(separator = ',')

    implicit val csvParser = new CsvParser[Cell] {

      override def lineCsvParser = lineCsvParserObject

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

  it should "return a parsing error" in new WithParsingError {
    parsingError.line should be (line)
    parsingError.error should be (error)
    parsingError.typeValue should be (errorMessage)
  }
}
