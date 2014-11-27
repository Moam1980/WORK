/*
 * TODO: License goes here!
 */

package sa.com.mobily.flickering

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell.{Cell, FourGTdd, Macro}
import sa.com.mobily.geometry.UtmCoordinates

class FlickeringCellsTest extends FlatSpec with ShouldMatchers {

  trait WithFlickeringCell {

    val sridPlanar = 32638
    val shapeWkt1 = "POLYGON (( 0 0, 0 4, 2 4, 2 0, 0 0 ))"
    val shapeWkt2 = "POLYGON (( 3 4, 7 4, 7 0, 3 4 ))"

    val fields =
      Array("57", "4465390", "57", "4465391", "MULTIPOLYGON (((0 0, 0 4, 2 4, 2 0, 0 0)), ((3 4, 7 4, 7 0, 3 4)))")

    val cell1 = Cell(
      mcc = "420",
      mnc = "03",
      cellId = 4465390,
      lacTac = 57,
      planarCoords = UtmCoordinates(-194243.4, 2671697.6, "EPSG:32638"),
      technology = FourGTdd,
      cellType = Macro,
      height = 25,
      azimuth = 0,
      beamwidth = 90,
      range = 2530.3,
      coverageWkt = shapeWkt1)

    val cell2 = cell1.copy(cellId = 4465391, coverageWkt = shapeWkt2)
    val flickeringCells = FlickeringCells(Set((cell1.lacTac, cell1.cellId), (cell2.lacTac, cell2.cellId)))
    val cellCatalogue = Map(((cell1.lacTac, cell1.cellId), cell1), ((cell2.lacTac, cell2.cellId), cell2))
  }

  "FlickeringCell" should "generate a list of fields" in new WithFlickeringCell {
    flickeringCells.fields(cellCatalogue) should be(fields)
  }
}
