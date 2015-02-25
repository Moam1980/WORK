/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import org.scalatest._

import sa.com.mobily.parsing.CsvParser

class ImsNeighboringCellTest extends FlatSpec with ShouldMatchers {

  import ImsNeighboringCell._

  trait WithImsNeighboringCell {
    val imsNeighboringCellLine = "\"ALU\"|\"2G\"|\"AJH3440_1\"|\"UNBREA2244_A\"|\"-103\""
    val fields = Array("ALU", "2G", "AJH3440_1", "UNBREA2244_A", "-103")

    val header =
      Array("vendor", "technology", "cellName", "neighboringCellName", "qrxLevMin")
    val imsNeighboringCellFields =
      Array("ALU", "2G", "AJH3440_1", "UNBREA2244_A", "-103")

    val imsNeighboringCell = ImsNeighboringCell("ALU", TwoG, "AJH3440_1", "UNBREA2244_A", Some(-103))
  }
  
  "ImsNeighboringCell" should "be built from CSV" in new WithImsNeighboringCell {
    CsvParser.fromLine(imsNeighboringCellLine).value.get should be (imsNeighboringCell)
  }

  it should "be discarded when the CSV format is wrong" in new WithImsNeighboringCell {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(1, "NotTechnology"))
  }

  it should "return correct header" in new WithImsNeighboringCell {
    ImsNeighboringCell.Header should be (header)
  }

  it should "return correct fields" in new WithImsNeighboringCell {
    imsNeighboringCell.fields should be (imsNeighboringCellFields)
  }

  it should "be built from CSV when qrxLevMin not valid" in new WithImsNeighboringCell {
    fromCsv.fromFields(fields.updated(4, "RXLEVMIN")) should
      be (imsNeighboringCell.copy(qrxLevMin = None))
  }
}
