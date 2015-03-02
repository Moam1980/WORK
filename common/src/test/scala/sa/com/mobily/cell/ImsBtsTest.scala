/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import org.scalatest._

class ImsBtsTest extends FlatSpec with ShouldMatchers {

  trait WithImsBts {

    val header =
      Array("vendor", "technology", "id", "node", "parentBsc", "longitude", "latitude", "ipAddress", "netmask")

    val imsBtsFields = Array("ALU", "2G", "AJH3440", "AJH3440", "E371_ARAR3", "", "", "", "")

    val imsBts = ImsBts("ALU", TwoG, "AJH3440", "E371_ARAR3", "", "", "", "")
  }

  trait WithImsBtsToMerge {
    val imsBts1 = ImsBts("ERICSSON", TwoG, "AAZIZRD_328_G12-TG-83_TU8D107712", "E111", "", "", "IP_Address", "NETMASK")

    val imsBts2 =
      ImsBts("ERICSSON", TwoG, "AAZIZRD_328_G12-TG-83_TU8D107712", "E111", "N24-48-21.380", "E24-48-21.380", "", "")

    val imsBtsMerged =
      ImsBts("ERICSSON", TwoG, "AAZIZRD_328_G12-TG-83_TU8D107712", "E111", "N24-48-21.380", "E24-48-21.380",
        "IP_Address", "NETMASK")
  }

  it should "return correct header" in new WithImsBts {
    ImsBts.Header should be (header)
  }

  it should "return correct fields" in new WithImsBts {
    imsBts.fields should be (imsBtsFields)
  }

  it should "parse id from fields with composite node" in new WithImsBts {
    ImsBts.parseIdFromNode("WHANIFAH_763_G12-TG-265_CC45087127") should be ("763")
  }

  it should "parse id from fields with composite node with more fields at the beginning" in new WithImsBts {
    ImsBts.parseIdFromNode("WEST_SAWAD_1581_G12-TG-41_AB20043556") should be ("1581")
  }

  it should "parse id from fields with composite node and id starting in G" in new WithImsBts {
    ImsBts.parseIdFromNode("JD0A_KAIA_G2324_G12-TG-46_CC47911733") should be ("2324")
  }

  it should "merge two bts getting fields from second one" in new WithImsBtsToMerge {
    ImsBts.merge(imsBts1, imsBts2) should be (imsBtsMerged)
  }

  it should "merge two bts getting fields from first one" in new WithImsBtsToMerge {
    ImsBts.merge(imsBts2, imsBts1) should be (imsBtsMerged)
  }

  it should "throw an exception if vendor is not the same when merging" in new WithImsBtsToMerge {
    an [Exception] should be thrownBy ImsBts.merge(imsBts1, imsBts2.copy(vendor = "other"))
  }

  it should "throw an exception if technology is not the same when merging" in new WithImsBtsToMerge {
    an [Exception] should be thrownBy ImsBts.merge(imsBts1, imsBts2.copy(technology = ThreeG))
  }

  it should "throw an exception if node is not the same when merging" in new WithImsBtsToMerge {
    an [Exception] should be thrownBy ImsBts.merge(imsBts1, imsBts2.copy(node = "other"))
  }
}
