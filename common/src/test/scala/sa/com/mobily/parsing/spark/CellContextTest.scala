/*
 * TODO: License goes here!
 */

package sa.com.mobily.parsing.spark

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell.spark.CellContext
import sa.com.mobily.utils.LocalSparkContext

class CellContextTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import CellContext._

  trait WithCells {

    val cells = sc.parallelize(List("2142342,21.62,39.16", "NaN,21.62,39.16", "1923828,26.43,50.09"))
  }

  "CellContext" should "get correctly parsed cells" in new WithCells {
    cells.toCell.count should be (2)
  }

  it should "get errors when parsing cells" in new WithCells {
    cells.toCellErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed cells" in new WithCells {
    cells.toParsedCell.count should be (3)
  }
}
