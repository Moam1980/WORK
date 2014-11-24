/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.flickering

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell.Flickering

class FlickeringTest extends FlatSpec with ShouldMatchers {

  trait WithCells {

    val cell1 = (1, 1)
    val cell2 = (2, 2)
    val cell3 = (1, 3)
    val cell4 = (2, 3)
    val cell5 = (3, 3)
    val cell6 = (3, 4)
  }

  trait WithFlickeringCells extends WithCells {

    val timeCell1 = (1L, cell1)
    val timeCell2 = (2L, cell2)
    val timeCell3 = (3L, cell1)
    val timeCell4 = (4L, cell2)
    val timeCell5 = (5L, cell3)
    val timeCell6 = (7L, cell2)
    val timeCell7 = (7L, cell3)
    val timeCell8 = (8L, cell1)

    val timeCells = List(timeCell1, timeCell2, timeCell3, timeCell4, timeCell5, timeCell6, timeCell7, timeCell8)
    val flickeringCells = Set(Set(cell1, cell2), Set(cell2, cell3), Set(cell1,cell3))
  }

  trait WithOneRepeatedCell extends WithCells {

    val timeCell1 = (1L, cell1)
    val timeCell2 = (2L, cell1)
    val timeCell3 = (3L, cell1)
    val timeCell4 = (4L, cell1)
    val timeCell5 = (5L, cell1)
    val timeCell6 = (6L, cell2)

    val timeCells = List(timeCell1, timeCell2, timeCell3, timeCell4, timeCell5, timeCell6)
  }

  trait WithNonFlickeringCells extends WithCells {

    val timeCell1 = (1L, cell1)
    val timeCell2 = (2L, cell2)
    val timeCell3 = (3L, cell3)
    val timeCell4 = (4L, cell4)
    val timeCell5 = (5L, cell5)
    val timeCell6 = (6L, cell6)

    val timeCells = List(timeCell1, timeCell2, timeCell3, timeCell4, timeCell5, timeCell6)
  }

  "FlickeringDetector" should "detect flickering" in new WithFlickeringCells {
    val flickering = Flickering.detect(timeCells, 5)
    flickering.size should be (3)
    flickering should be (flickeringCells)
  }

  it should "not detect flickering in range with one cell" in new WithOneRepeatedCell {
    val flickering = Flickering.detect(timeCells, 5)
    flickering.size should be (0)
  }

  it should "not detect flickering in range with non flickering cells" in new WithNonFlickeringCells  {
    val flickering = Flickering.detect(timeCells, 5)
    flickering.size should be (0)
  }
}
