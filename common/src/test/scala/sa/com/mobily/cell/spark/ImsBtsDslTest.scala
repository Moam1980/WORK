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

    val imsBts1 = ImsBts("ALU", TwoG, "AJH3440", "E371_ARAR3", "", "", "", "")
    val imsBts2 =
      ImsBts("ERICSSON", TwoG, "RDV_1886_G12-TG-15_CB4F619275", "E116", "N24-51-28.120", "E24-51-28.120", "", "")
    val imsBts3 = ImsBts("ERICSSON", TwoG, "RDV_1886_G12-TG-15_CB4F619275", "E116", "", "", "IP_Address", "NETMASK")

    val imsBtsMerged =
      ImsBts("ERICSSON", TwoG, "RDV_1886_G12-TG-15_CB4F619275", "E116", "N24-51-28.120", "E24-51-28.120", "IP_Address",
        "NETMASK")

    val imsBtsRdd = sc.parallelize(Array(imsBts1, imsBts2, imsBts3))
    val imsBtsMergedRdd = sc.parallelize(Array(imsBts1, imsBtsMerged))
  }

  it should "merge ims cells" in new WithImsBtsText {
    imsBtsRdd.mergeImsBts.collect should contain theSameElementsAs (imsBtsMergedRdd.collect)
  }
}
