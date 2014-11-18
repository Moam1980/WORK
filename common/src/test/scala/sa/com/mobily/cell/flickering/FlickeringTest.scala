/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.flickering

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell.Flickering

class FlickeringTest extends FlatSpec with ShouldMatchers {

  trait WithFlickeringCells {

    val cell1 = (1L, (1, 1))
    val cell2 = (2L, (2, 2))
    val cell3 = (3L, (1, 1))
    val cell4 = (4L, (2, 2))
    val cell5 = (5L, (1, 3))
    val cell6 = (7L, (2, 2))
    val cell7 = (7L, (1, 3))
    val cell8 = (8L, (1, 1))

    val cells = List(cell1, cell2, cell3, cell4, cell5, cell6, cell7, cell8)
    val cellsSet1 = Set(cell1._2, cell2._2)
    val cellsSet2 = Set(cell5._2, cell6._2)
  }

  "Flickering" should "detect flickering" in new WithFlickeringCells {
    val flick = Flickering.detect(cells, 2)
    flick.length should be (2)
    flick should be (Seq(cellsSet1, cellsSet2))
  }
}
