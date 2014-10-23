/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.geometry.{GeomUtils, UtmCoordinates}
import sa.com.mobily.parsing.CsvParser
import sa.com.mobily.utils.EdmCustomMatchers

class CellTest extends FlatSpec with ShouldMatchers {

  import Cell._

  trait WithCell {

    val sridPlanar = 32638
    val shapeWkt = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"
    val cellLine = "420|03|4465390|57|-194243.4|2671697.6|EPSG:32638|4G_TDD|MACRO|25.0|0.0|90.0|2530.3|" + shapeWkt
    val fields = Array("420", "03", "4465390", "57", "-194243.4", "2671697.6", "EPSG:32638", "4G_TDD", "MACRO", "25.0",
      "0.0", "90.0", "2530.3", shapeWkt)
    val cell = Cell(
      mcc = "420",
      mnc = "03",
      cellId = "4465390",
      lac = 57,
      planarCoords = UtmCoordinates(-194243.4, 2671697.6, "EPSG:32638"),
      technology = FourGTdd,
      cellType = Macro,
      height = 25,
      azimuth = 0,
      beamwidth = 90,
      range = 2530.3,
      coverageWkt = shapeWkt)
  }

  "Cell" should "be built from CSV" in new WithCell {
    CsvParser.fromLine[Cell](cellLine).value.get should be (cell)
  }

  it should "be discarded when the CSV format is wrong" in new WithCell {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(7, "NotValid"))
  }

  it should "be built from CSV with a 2G cell" in new WithCell {
    fromCsv.fromFields(fields.updated(7, "2G")) should be (cell.copy(technology = TwoG))
  }

  it should "be built from CSV with a 3G cell" in new WithCell {
    fromCsv.fromFields(fields.updated(7, "3G")) should be (cell.copy(technology = ThreeG))
  }

  it should "be built from CSV with a 4G FDD cell" in new WithCell {
    fromCsv.fromFields(fields.updated(7, "4G_FDD")) should be (cell.copy(technology = FourGFdd))
  }

  it should "be built from CSV with a 4G TDD cell" in new WithCell {
    fromCsv.fromFields(fields.updated(7, "4G_TDD")) should be (cell.copy(technology = FourGTdd))
  }

  it should "be built from CSV with RDU type" in new WithCell {
    fromCsv.fromFields(fields.updated(8, "Rdu")) should be (cell.copy(cellType = Rdu))
  }

  it should "be built from CSV with CRANE type" in new WithCell {
    fromCsv.fromFields(fields.updated(8, "Crane")) should be (cell.copy(cellType = Crane))
  }

  it should "be built from CSV with MACRO type" in new WithCell {
    fromCsv.fromFields(fields.updated(8, "Macro")) should be (cell.copy(cellType = Macro))
  }

  it should "be built from CSV with RT type" in new WithCell {
    fromCsv.fromFields(fields.updated(8, "Rt")) should be (cell.copy(cellType = Rt))
  }

  it should "be built from CSV with MICRO type" in new WithCell {
    fromCsv.fromFields(fields.updated(8, "Micro")) should be (cell.copy(cellType = Micro))
  }

  it should "be built from CSV with TOWER type" in new WithCell {
    fromCsv.fromFields(fields.updated(8, "Tower")) should be (cell.copy(cellType = Tower))
  }

  it should "be built from CSV with INDOOR type" in new WithCell {
    fromCsv.fromFields(fields.updated(8, "Indoor")) should be (cell.copy(cellType = Indoor))
  }

  it should "be built from CSV with MONOPOLE type" in new WithCell {
    fromCsv.fromFields(fields.updated(8, "Monopole")) should be (cell.copy(cellType = Monopole))
  }

  it should "be built from CSV with RDS type" in new WithCell {
    fromCsv.fromFields(fields.updated(8, "Rds")) should be (cell.copy(cellType = Rds))
  }

  it should "be built from CSV with PALM TREE type" in new WithCell {
    fromCsv.fromFields(fields.updated(8, "Palm Tree")) should be (cell.copy(cellType = PalmTree))
  }

  it should "be built from CSV with OUTLET type" in new WithCell {
    fromCsv.fromFields(fields.updated(8, "Outlet")) should be (cell.copy(cellType = Outlet))
  }

  it should "be built from CSV with PARKING type" in new WithCell {
    fromCsv.fromFields(fields.updated(8, "Parking")) should be (cell.copy(cellType = Parking))
  }

  it should "be built from CSV with PICO type" in new WithCell {
    fromCsv.fromFields(fields.updated(8, "PICO")) should be (cell.copy(cellType = Pico))
  }

  it should "provide the coverage geometry" in new WithCell with EdmCustomMatchers {
    cell.coverageGeom should equalGeometry (GeomUtils.parseWkt(shapeWkt, sridPlanar))
  }

  it should "generate a list of fields" in new WithCell {
    Cell.toFields(cell) should be (fields)
  }
}
