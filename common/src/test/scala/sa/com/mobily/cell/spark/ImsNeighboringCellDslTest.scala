/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import org.scalatest._

import sa.com.mobily.utils.LocalSparkContext

class ImsNeighboringCellDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import ImsNeighboringCellDsl._

  trait WithImsNeighboringCellText {

    val imsNeighboringCellLine1 = "\"ALU\"|\"2G\"|\"AJH3440_1\"|\"UNBREA2244_A\"|\"-103\""
    val imsNeighboringCellLine2 = "\"ERICSSON\"|\"3G\"|\"UXAV3087B\"|\"UXAV3087C\"|\"-115\""
    val imsNeighboringCellLine3 = "\"ERICSSON\"|\"NotATech\"|\"UXAV3087B\"|\"UXAV3087C\"|\"NaN\""

    val imsNeighboringCell =
      sc.parallelize(List(imsNeighboringCellLine1, imsNeighboringCellLine2, imsNeighboringCellLine3))
  }

  "ImsNeighboringCellContext" should "get correctly parsed IMS neighboring cell" in new WithImsNeighboringCellText {
    imsNeighboringCell.toImsNeighboringCell.count should be (2)
  }

  it should "get errors when parsing IMS neighboring cell" in new WithImsNeighboringCellText {
    imsNeighboringCell.toImsNeighboringCellErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed IMS neighboring cell" in new WithImsNeighboringCellText {
    imsNeighboringCell.toParsedImsNeighboringCell.count should be (3)
  }
}
