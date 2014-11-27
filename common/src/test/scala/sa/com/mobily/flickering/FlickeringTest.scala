/*
 * TODO: License goes here!
 */

package sa.com.mobily.flickering

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell._
import sa.com.mobily.geometry.UtmCoordinates

class FlickeringTest extends FlatSpec with ShouldMatchers {

  trait WithCells {

    val shapeWkt1 = "POLYGON (( 0 0, 0 4, 2 4, 2 0, 0 0 ))"
    val shapeWkt2 = "POLYGON (( 3 4, 7 4, 7 0, 3 4 ))"
    val shapeWkt3 = "POLYGON (( 3 4, 7 4, 7 0, 5 0, 3 4 ))"
    val shapeWkt4 = "POLYGON (( 3 4, 7 4, 7 0, 4 0, 4 2, 3 2, 3 4 ))"
    val shapeWkt5 = "POLYGON (( 0 5, 0 7, 2 7, 2 5, 0 5 ))"
    val shapeWkt6 = "POLYGON (( 1 0, 1 2, 3 2, 3 0, 1 0 ))"

    val cell1 = Cell(
      mcc = "420",
      mnc = "03",
      cellId = 4465390,
      lacTac = 51,
      planarCoords = UtmCoordinates(-194243.4, 2671697.6, "EPSG:32638"),
      technology = FourGTdd,
      cellType = Macro,
      height = 25,
      azimuth = 0,
      beamwidth = 90,
      range = 2530.3,
      coverageWkt = shapeWkt1)
    val cell2 = cell1.copy(lacTac = 52, coverageWkt = shapeWkt2)
    val cell3 = cell1.copy(lacTac = 53, coverageWkt = shapeWkt3)
    val cell4 = cell1.copy(lacTac = 54, coverageWkt = shapeWkt4)
    val cell5 = cell1.copy(lacTac = 55, coverageWkt = shapeWkt5)
    val cell6 = cell1.copy(lacTac = 56, coverageWkt = shapeWkt6)

    val cellCatalogue = Map(
      ((cell1.lacTac, cell1.cellId), cell1),
      ((cell2.lacTac, cell2.cellId), cell2),
      ((cell3.lacTac, cell3.cellId), cell3),
      ((cell4.lacTac, cell4.cellId), cell4),
      ((cell5.lacTac, cell5.cellId), cell5),
      ((cell6.lacTac, cell6.cellId), cell6))
  }

  trait WithFlickeringCells extends WithCells {

    val timeCell1 = (1L, (cell1.lacTac, cell1.cellId))
    val timeCell2 = (2L, (cell2.lacTac, cell2.cellId))
    val timeCell3 = (3L, (cell1.lacTac, cell1.cellId))
    val timeCell4 = (4L, (cell2.lacTac, cell2.cellId))
    val timeCell5 = (5L, (cell3.lacTac, cell3.cellId))
    val timeCell6 = (7L, (cell2.lacTac, cell2.cellId))
    val timeCell7 = (7L, (cell3.lacTac, cell3.cellId))
    val timeCell8 = (8L, (cell1.lacTac, cell1.cellId))

    val timeCells = List(timeCell1, timeCell2, timeCell3, timeCell4, timeCell5, timeCell6, timeCell7, timeCell8)
    val flickeringCell1 = (cell1.lacTac, cell1.cellId)
    val flickeringCell2 = (cell2.lacTac, cell2.cellId)
    val flickeringCell3 = (cell3.lacTac, cell3.cellId)

    val flickeringCells = Set(
      FlickeringCells(Set(flickeringCell1, flickeringCell2)),
      FlickeringCells(Set(flickeringCell2, flickeringCell3)),
      FlickeringCells(Set(flickeringCell1, flickeringCell3)))
  }

  trait WithOneRepeatedCell extends WithCells {

    val timeCell1 = (1L, (cell1.lacTac, cell1.cellId))
    val timeCell2 = (2L, (cell1.lacTac, cell1.cellId))
    val timeCell3 = (3L, (cell1.lacTac, cell1.cellId))
    val timeCell4 = (4L, (cell1.lacTac, cell1.cellId))
    val timeCell5 = (5L, (cell1.lacTac, cell1.cellId))
    val timeCell6 = (6L, (cell2.lacTac, cell2.cellId))

    val timeCells = List(timeCell1, timeCell2, timeCell3, timeCell4, timeCell5, timeCell6)
  }

  trait WithNonFlickeringCells extends WithCells {

    val timeCell1 = (1L, (cell1.lacTac, cell1.cellId))
    val timeCell2 = (2L, (cell2.lacTac, cell2.cellId))
    val timeCell3 = (3L, (cell3.lacTac, cell3.cellId))
    val timeCell4 = (4L, (cell4.lacTac, cell4.cellId))
    val timeCell5 = (5L, (cell5.lacTac, cell5.cellId))
    val timeCell6 = (6L, (cell6.lacTac, cell6.cellId))

    val timeCells = List(timeCell1, timeCell2, timeCell3, timeCell4, timeCell5, timeCell6)
  }

  "FlickeringDetector" should "detect flickering" in new WithFlickeringCells {
    val flickering = Flickering.detect(timeCells, 5)(cellCatalogue)
    flickering.size should be (3)
    flickering should be(flickeringCells)
  }

  it should "not detect flickering in range with one cell" in new WithOneRepeatedCell {
    val flickering = Flickering.detect(timeCells, 5)(cellCatalogue)
    flickering.size should be(0)
  }

  it should "not detect flickering in range with non flickering cells" in new WithNonFlickeringCells {
    val flickering = Flickering.detect(timeCells, 5)(cellCatalogue)
    flickering.size should be(0)
  }
}
