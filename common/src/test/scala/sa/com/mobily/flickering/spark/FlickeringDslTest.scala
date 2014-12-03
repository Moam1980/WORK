/*
 * TODO: License goes here!
 */

package sa.com.mobily.flickering.spark

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell._
import sa.com.mobily.flickering.FlickeringCells
import sa.com.mobily.geometry.UtmCoordinates
import sa.com.mobily.utils.LocalSparkContext

class FlickeringDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import FlickeringDsl._

  trait WithOverlappingCells {

    val shapeWkt1 = "POLYGON (( 0 0, 0 4, 2 4, 2 0, 0 0 ))"
    val shapeWkt2 = "POLYGON (( 0 5, 0 9, 2 9, 2 5, 0 5 ))"
    val shapeWkt3 = "POLYGON (( 1 6, 1 10, 3 10, 3 6, 1 6 ))"
    val shapeWkt4 = "POLYGON (( 10 60, 10 100, 30 100, 30 60, 10 60 ))"

    val cell1 = Cell(
      cellId = 1,
      lacTac = 1,
      planarCoords = UtmCoordinates(0, 0, "EPSG:32638"),
      technology = ThreeG,
      cellType = Macro,
      height = 22.0,
      azimuth = 0.0,
      beamwidth =  168.0,
      range = 10.0,
      coverageWkt = shapeWkt1,
      mcc = "420",
      mnc = "03")
    val cell2 = cell1.copy(
      cellId = 2,
      lacTac = 2,
      planarCoords = UtmCoordinates(0, 5, "EPSG:32638"),
      coverageWkt = shapeWkt2)
    val cell3 = cell1.copy(
      cellId = 3,
      lacTac = 3,
      planarCoords = UtmCoordinates(1, 6, "EPSG:32638"),
      coverageWkt = shapeWkt3)
    val cell4 = cell1.copy(
      cellId = 4,
      lacTac = 4,
      planarCoords = UtmCoordinates(10, 60, "EPSG:32638"),
      azimuth = 120.0,
      coverageWkt = shapeWkt4)
    val cell1Ids = (cell1.lacTac, cell1.cellId)
    val cell2Ids = (cell2.lacTac, cell2.cellId)
    val cell3Ids = (cell3.lacTac, cell3.cellId)
    val cell4Ids = (cell4.lacTac, cell4.cellId)
    val cellCatalogue = Map((cell1Ids, cell1), (cell2Ids, cell2), (cell3Ids, cell3), (cell4Ids, cell4))
  }

  trait WithFlickeringCells extends WithOverlappingCells{

    val flickeringCells1 = FlickeringCells(Set(cell1Ids, cell2Ids))
    val flickeringCells2 = FlickeringCells(Set(cell2Ids, cell3Ids))
    val flickeringCells3 = FlickeringCells(Set(cell4Ids, cell3Ids))
    val flickeringCells = sc.parallelize(Seq(flickeringCells1, flickeringCells2, flickeringCells3))
    val bcCellCatalogue = sc.broadcast(cellCatalogue)
  }

  trait WithRepeatedFlickeringCells extends WithOverlappingCells{

    val flickeringCells1 = FlickeringCells(Set(cell3Ids, cell4Ids))
    val flickeringCells2 = FlickeringCells(Set(cell1Ids, cell2Ids))
    val flickeringCells3 = FlickeringCells(Set(cell1Ids, cell3Ids))
    val flickeringCells4 = FlickeringCells(Set(cell1Ids, cell4Ids))
    val flickeringCells = sc.parallelize(Seq(flickeringCells1, flickeringCells2, flickeringCells3, flickeringCells4))
  }

  "FlickeringDsl" should "filter the overlapping flickering cells" in new WithFlickeringCells {
    flickeringCells.filterNonIntersectedCells(bcCellCatalogue).collect.length should be (2)
  }

  it should "analyze the flickering" in new WithFlickeringCells {
    flickeringCells.flickeringAnalysis()(bcCellCatalogue).collect.length should be (2)
  }

  it should "analyze the solvable flickering" in new WithFlickeringCells {
    flickeringCells.solvableFlickering()(bcCellCatalogue).collect.length should be (1)
  }

  it should "analyze the non solvable flickering" in new WithFlickeringCells {
    flickeringCells.nonSolvableFlickering()(bcCellCatalogue).collect.length should be (1)
  }

  it should "count the number of flickerings by cell" in new WithRepeatedFlickeringCells {
    val flickeringCounter = flickeringCells.countFlickeringsByCell.collect
    flickeringCounter.size should be (4)
    flickeringCounter should contain (cell1Ids, 3)
    flickeringCounter should contain (cell2Ids, 1)
    flickeringCounter should contain (cell3Ids, 2)
    flickeringCounter should contain (cell4Ids, 2)
  }
}
