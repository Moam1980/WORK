/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import org.scalatest._

import sa.com.mobily.parsing.CsvParser

class ImsCellTest extends FlatSpec with ShouldMatchers {

  import ImsCell._

  trait WithImsCell {

    val imsCellLine = "\"ALU\"|\"2G\"|\"HUFEA2173_2\"|\"21732\"|\"33\"|\"NULL\"|\"NULL\"|\"NULL\"|\"24\"|\"NULL\""
    val fields = Array("ALU", "2G", "HUFEA2173_2", "21732", "33", "NULL", "NULL", "NULL", "24", "NULL")

    val header =
      Array("vendor", "technology", "nodeB", "cellName", "cellId", "bcchFrequency", "lac", "rac", "sac", "maxTxPower",
        "extra")
    val imsCellFields =
      Array("ALU", "2G", "HUFEA2173", "HUFEA2173_2", "21732", "33", "", "", "", "24", "")

    val imsCell = ImsCell("ALU", TwoG, "HUFEA2173", "HUFEA2173_2", "21732", Some(33), None, None, None, Some(24), "")
  }
  
  "ImsCell" should "be built from CSV" in new WithImsCell {
    CsvParser.fromLine(imsCellLine).value.get should be (imsCell)
  }

  it should "be discarded when the CSV format is wrong" in new WithImsCell {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(1, "NotTechnology"))
  }

  it should "return correct header" in new WithImsCell {
    ImsCell.header should be (header)
  }

  it should "return correct fields" in new WithImsCell {
    imsCell.fields should be (imsCellFields)
  }

  it should "parse nodeB from fields with simple cell name" in new WithImsCell {
    fromCsv.fromFields(fields.updated(2, "1088C")) should be (imsCell.copy(nodeB = "1088", cellName = "1088C"))
  }

  it should "parse nodeB from fields with cell name containing '_'" in new WithImsCell {
    fromCsv.fromFields(fields.updated(2, "HUFEA2729_2")) should
      be (imsCell.copy(nodeB = "HUFEA2729", cellName = "HUFEA2729_2"))
  }

  it should "parse nodeB from fields with cell name containing '-'" in new WithImsCell {
    fromCsv.fromFields(fields.updated(2, "MU3166-1")) should be (imsCell.copy(nodeB = "MU3166", cellName = "MU3166-1"))
  }

  it should "parse nodeB from fields with vip cell name and '_'" in new WithImsCell {
    fromCsv.fromFields(fields.updated(2, "VIP_U1741_G")) should
      be (imsCell.copy(nodeB = "VIP_U1741", cellName = "VIP_U1741_G"))
  }

  it should "parse nodeB from fields with vip cell name and not '_'" in new WithImsCell {
    fromCsv.fromFields(fields.updated(2, "VIP_U6401G")) should
      be (imsCell.copy(nodeB = "VIP_U6401", cellName = "VIP_U6401G"))
  }
}
