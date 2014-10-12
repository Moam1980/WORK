/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import org.scalatest.{FlatSpec, ShouldMatchers}
import sa.com.mobily.parsing.CsvParser

class GisSqmCellTest extends FlatSpec with ShouldMatchers {

  import GisSqmCell._

  trait WithGisSqmCell {
    val gisSqmCellLine = "420030104849823|03|C4982C|42003010484982|102|K742266V02|42|900|BTS|2G|Macro|21.7"
    val fields = Array("420030104849823", "03", "C4982C", "42003010484982", "102", "K742266V02", "42", "900",
      "BTS", "2G", "Macro", "21.7")
    val gisSqmCell = GisSqmCell("420030104849823", "03", "C4982C", "42003010484982", 102, "K742266V02", "42", "900",
      "BTS", TwoG, Macro, 21.7)
  }

  "GisSqmCell" should "be built from CSV" in new WithGisSqmCell {
    CsvParser.fromLine(gisSqmCellLine).value.get should be (gisSqmCell)
  }

  it should "be discarded when the CSV format is wrong" in new WithGisSqmCell {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(10, "WrongTech"))
  }
}
