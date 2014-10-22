/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import org.scalatest.{FlatSpec, ShouldMatchers}
import sa.com.mobily.utils.LocalSparkContext

class CellDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import CellDsl._

  trait WithCellsText {

    val cell1 = "420030145704439|21.62|39.16|3G|Macro|38|90|15|900|90"
    val cell2 = "420030010520161|NaN|39.16|4G_FDD|Micro|38|90|15|900|90"
    val cell3 = "420030021422424|26.43|50.09|4G_TDD|Indoor|38|90|15|1800|90|POLYGON (( 0 0, 1 0, 1 1, 0 1, 0 0 ))"

    val cells = sc.parallelize(List(cell1, cell2, cell3))
  }

  "CellContext" should "get correctly parsed cells" in new WithCellsText {
    cells.toCell.count should be (2)
  }

  it should "get errors when parsing cells" in new WithCellsText {
    cells.toCellErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed cells" in new WithCellsText {
    cells.toParsedCell.count should be (3)
  }
}
