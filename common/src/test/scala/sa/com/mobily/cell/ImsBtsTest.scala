/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import org.scalatest._

import sa.com.mobily.parsing.CsvParser

class ImsBtsTest extends FlatSpec with ShouldMatchers {

  import ImsBts._

  trait WithImsBts {
    val imsBtsLine =
      "\"ALU\"|\"2G\"|\"AJH3440\"|\"E371_ARAR3\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|\"3833\"|\"420\"|\"03\""
    val fields = Array("ALU", "2G", "AJH3440", "E371_ARAR3", "NULL", "NULL", "NULL", "NULL", "3833", "420", "03")

    val header =
      Array("vendor", "technology", "id", "nodeB", "parentBsc", "longitude", "latitude", "ipAddress", "netmask",
        "lac", "mcc", "mnc")
    val imsBtsFields = Array("ALU", "2G", "AJH3440", "AJH3440", "E371_ARAR3", "", "", "", "", "3833", "420", "03")

    val imsBts = ImsBts("ALU", TwoG, "AJH3440", "AJH3440", "E371_ARAR3", "", "", "", "", Some(3833), "420", "03")
  }

  trait WithImsBtsToMerge {
    val imsBts1 =
      ImsBts("ERICSSON", TwoG, "328", "AAZIZRD_328_G12-TG-83_TU8D107712", "E111", "", "", "IP_Address", "NETMASK",
        None, "", "")

    val imsBts2 =
      ImsBts("ERICSSON", TwoG, "328", "AAZIZRD_328_G12-TG-83_TU8D107712", "E111", "N24-48-21.380", "E24-48-21.380",
        "", "", Some(1072), "420", "3")

    val imsBtsMerged =
      ImsBts("ERICSSON", TwoG, "328", "AAZIZRD_328_G12-TG-83_TU8D107712", "E111", "N24-48-21.380", "E24-48-21.380",
        "IP_Address", "NETMASK", Some(1072), "420", "3")
  }
  
  "ImsBts" should "be built from CSV" in new WithImsBts {
    CsvParser.fromLine(imsBtsLine).value.get should be (imsBts)
  }

  it should "be discarded when the CSV format is wrong" in new WithImsBts {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(1, "NotTechnology"))
  }

  it should "return correct header" in new WithImsBts {
    ImsBts.header should be (header)
  }

  it should "return correct fields" in new WithImsBts {
    imsBts.fields should be (imsBtsFields)
  }

  it should "parse id from fields with composite nodeB" in new WithImsBts {
    fromCsv.fromFields(fields.updated(2, "WHANIFAH_763_G12-TG-265_CC45087127")) should
      be (imsBts.copy(id = "763", nodeB = "WHANIFAH_763_G12-TG-265_CC45087127"))
  }

  it should "parse id from fields with composite nodeB with more fields at the beginning" in new WithImsBts {
    fromCsv.fromFields(fields.updated(2, "WEST_SAWAD_1581_G12-TG-41_AB20043556")) should
      be (imsBts.copy(id = "1581", nodeB = "WEST_SAWAD_1581_G12-TG-41_AB20043556"))
  }

  it should "parse id from fields with composite nodeB and id starting in G" in new WithImsBts {
    fromCsv.fromFields(fields.updated(2, "JD0A_KAIA_G2324_G12-TG-46_CC47911733")) should
      be (imsBts.copy(id = "2324", nodeB = "JD0A_KAIA_G2324_G12-TG-46_CC47911733"))
  }

  it should "merge two bts getting fields from second one" in new WithImsBtsToMerge {
    ImsBts.merge(imsBts1, imsBts2) should be (imsBtsMerged)
  }

  it should "merge two bts getting fields from first one" in new WithImsBtsToMerge {
    ImsBts.merge(imsBts2, imsBts1) should be (imsBtsMerged)
  }
}
