/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import org.scalatest._
import sa.com.mobily.utils.LocalSparkContext

class ImsCellDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import ImsCellDsl._

  trait WithImsCellText {

    val imsCellLine1 = "\"ALU\"|\"2G\"|\"HUFEA2691_6\"|\"26916\"|\"847\"|\"NULL\"|\"NULL\"|\"NULL\"|\"24\"|\"NULL\""
    val imsCellLine2 = 
      "\"ERICSSON\"|\"2G\"|\"1097A\"|\"10971\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|\"44\"|" +
        "\"ANTENNA_TYPE: OMNI CELL_DIR:  360 CELL_TYPE: MACRO BAND:GSM900 ACCMIN: 102 TALIM: 62\""
    val imsCellLine3 =
      "\"ERICSSON\"|\"NotATech\"|\"1097A\"|\"10971\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|\"44\"|" +
        "\"ANTENNA_TYPE: OMNI CELL_DIR:  360 CELL_TYPE: MACRO BAND:GSM900 ACCMIN: 102 TALIM: 62\""

    val imsCell = sc.parallelize(List(imsCellLine1, imsCellLine2, imsCellLine3))
  }

  "ImsCellContext" should "get correctly parsed IMS cell" in new WithImsCellText {
    imsCell.toImsCell.count should be (2)
  }

  it should "get errors when parsing IMS cell" in new WithImsCellText {
    imsCell.toImsCellErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed IMS cell" in new WithImsCellText {
    imsCell.toParsedImsCell.count should be (3)
  }
}
