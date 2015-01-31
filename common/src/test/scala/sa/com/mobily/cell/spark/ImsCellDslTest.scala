/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import org.scalatest._
import sa.com.mobily.utils.LocalSparkContext

class ImsCellDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import ImsCellDsl._

  trait WithImsCellText {

    val imsCellLine1 = "\"ALU\"|\"2G\"|\"AJH3440\"|\"NULL\"|\"NULL\"|\"E371_ARAR3\"|\"AJH3440_1\"|\"34401\"|\"NULL\"|" +
      "\"NULL\"|\"3833\"|\"NULL\"|\"NULL\"|\"420\"|\"03\"|\"40\"|\"24\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|" +
      "\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\""
    val imsCellLine2 = 
      "\"ERICSSON\"|\"2G\"|\"ALDOPAT_814_G12-TG-63_CC46247437\"|\"NULL\"|\"NULL\"|\"E115\"|\"814C\"|\"8143\"|" +
        "\"E24-40-16.180\"|\"N24-40-16.180\"|\"1055\"|\"NULL\"|\"NULL\"|\"420\"|\"3\"|\"NULL\"|\"42\"|\"MACRO\"|" +
        "\"GSM900\"|\"NULL\"|\"-500\"|\"0\"|\"0\"|\"62\"|\"360\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|\"102\"|" +
        "\"OMNI\"|\"NULL\"|\"NULL\""
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
