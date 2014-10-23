/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.geometry.LatLongCoordinates
import sa.com.mobily.parsing.CsvParser

class SqmCellTest extends FlatSpec with ShouldMatchers {

  import SqmCell._

  trait WithSqmCell {
    val sqmCellLine = "\"eNB_446539_0\",\"4465390\",\"6539\",\"4465390\",\"YB6539_P3_LTE\",\"57\",\"24.0056\"," +
      "\"38.1849\",\"HUAWEI\",\"4G_TDD\",\"MACRO\",\"25\",\"0\",\"M1P1 and M1P2 and M1P3 and M1P4 and M1P5\"," +
      "\"NORTH\",\"SECTOR\",15.2,-128,\"2600\""
    val fields = Array("\"eNB_446539_0\"", "\"4465390\"", "\"6539\"", "\"4465390\"", "\"YB6539_P3_LTE\"", "\"57\"",
      "\"24.0056\"", "\"38.1849\"", "\"HUAWEI\"", "\"4G_TDD\"", "\"MACRO\"", "\"25\"", "\"0\"",
      "\"M1P1 and M1P2 and M1P3 and M1P4 and M1P5\"", "\"NORTH\"", "\"SECTOR\"", "15.2", "-128", "\"2600\"")

    val coords = LatLongCoordinates("24.0056".toDouble, "38.1849".toDouble).utmCoordinates()

    val sqmCell = SqmCell("4465390", "4465390", "eNB_446539_0", "6539", "YB6539_P3_LTE", 57, coords, "HUAWEI",
      FourGTdd, Macro, 25.0, 0.0, "M1P1 and M1P2 and M1P3 and M1P4 and M1P5", "NORTH", "SECTOR", 15.2, -128, 2600)
  }

  "SqmCell" should "be built from CSV" in new WithSqmCell {
    CsvParser.fromLine(sqmCellLine).value.get should be (sqmCell)
  }

  it should "be discarded when the CSV format is wrong" in new WithSqmCell {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(10, "WrongCellType"))
  }
}
