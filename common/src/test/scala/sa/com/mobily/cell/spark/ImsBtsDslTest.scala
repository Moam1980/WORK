/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import org.scalatest._
import sa.com.mobily.cell.{TwoG, ImsBts}
import sa.com.mobily.utils.LocalSparkContext

class ImsBtsDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import ImsBtsDsl._

  trait WithImsBtsText {

    val imsBtsLine1 =
      "\"ALU\"|\"2G\"|\"AJH3440\"|\"E371_ARAR3\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|\"3833\"|\"420\"|\"03\""
    val imsBtsLine2 = 
      "\"ERICSSON\"|\"2G\"|\"RDV_1886_G12-TG-15_CB4F619275\"|\"E116\"|\"N24-51-28.120\"|\"E24-51-28.120\"|" +
        "\"NULL\"|\"NULL\"|\"1083\"|\"NULL\"|\"03\""
    val imsBtsLine3 =
      "\"ERICSSON\"|\"2G\"|\"RDV_1886_G12-TG-15_CB4F619275\"|\"E116\"|\"NULL\"|\"NULL\"|" +
        "\"IP_Address\"|\"NETMASK\"|\"NULL\"|\"420\"|\"NULL\""
    val imsBtsLine4 =
      "\"ERICSSON\"|\"NotATech\"|\"RDV_1886_G12-TG-15_CB4F619275\"|\"E116\"|\"N24-51-28.120\"|\"E24-51-28.120\"|" +
        "\"NULL\"|\"NULL\"|\"1083\"|\"420\"|\"03\""

    val imsBts1 = ImsBts("ALU", TwoG, "AJH3440", "AJH3440", "E371_ARAR3", "", "", "", "", Some(3833), "420", "03")
    val imsBts2 =
      ImsBts("ERICSSON", TwoG, "1886", "RDV_1886_G12-TG-15_CB4F619275", "E116", "N24-51-28.120", "E24-51-28.120",
        "IP_Address", "NETMASK", Some(1083), "420", "03")

    val imsBts = sc.parallelize(List(imsBtsLine1, imsBtsLine2, imsBtsLine3, imsBtsLine4))
    val imsBtsRdd = sc.parallelize(Array(imsBts2, imsBts1))
  }

  "ImsBtsContext" should "get correctly parsed IMS bts" in new WithImsBtsText {
    imsBts.toImsBts.count should be (3)
  }

  it should "get errors when parsing IMS bts" in new WithImsBtsText {
    imsBts.toImsBtsErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed IMS bts" in new WithImsBtsText {
    imsBts.toParsedImsBts.count should be (4)
  }

  it should "merge ims cells" in new WithImsBtsText {
    imsBts.toImsBts.mergeImsBts.collect should be (imsBtsRdd.collect)
  }
}
