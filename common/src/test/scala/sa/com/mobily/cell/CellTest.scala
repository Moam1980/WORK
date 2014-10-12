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
    val cellLineNoGeom = "420030145704439|21.62|39.16|3G|Macro|38|90|15|900|90"
    val shapeWkt =
      "POLYGON ((-105160.8 2402167.8, -105145.2 2402182.4, -105133.4 2402191.3, -105123.8 2402197.2, " +
        "-105115.2 2402201.4, -105107.3 2402204.4, -105099.9 2402206.5, -105093 2402207.7, -105086.4 2402208.2, " +
        "-105080.1 2402208.1, -105074.2 2402207.5, -105068.7 2402206.3, -105063.4 2402204.7, -105058.5 2402202.7, " +
        "-105054 2402200.3, -105049.8 2402197.5, -105046 2402194.5, -105042.5 2402191.1, -105039.4 2402187.5, " +
        "-105036.8 2402183.7, -105034.5 2402179.7, -105032.6 2402175.6, -105031.2 2402171.3, -105030.1 2402167, " +
        "-105029.5 2402162.6, -105029.3 2402158.1, -105029.5 2402153.6, -105030.1 2402149.2, -105031.2 2402144.9, " +
        "-105032.6 2402140.6, -105034.5 2402136.5, -105036.8 2402132.5, -105039.4 2402128.7, -105042.5 2402125.1, " +
        "-105046 2402121.7, -105049.8 2402118.7, -105054 2402115.9, -105058.5 2402113.5, -105063.4 2402111.5, " +
        "-105068.7 2402109.9, -105074.2 2402108.7, -105080.1 2402108.1, -105086.4 2402108, -105093 2402108.5, " +
        "-105099.9 2402109.7, -105107.3 2402111.8, -105115.2 2402114.8, -105123.8 2402119, -105133.4 2402124.9, " +
        "-105145.2 2402133.8, -105160.8 2402148.4, -105162.1 2402147.2, -105163.5 2402146.1, -105165.1 2402145.3, " +
        "-105166.7 2402144.6, -105168.4 2402144.2, -105170.2 2402143.9, -105172 2402143.9, -105173.8 2402144.2, " +
        "-105175.5 2402144.6, -105177.1 2402145.3, -105178.7 2402146.1, -105180.1 2402147.2, -105181.4 2402148.4, " +
        "-105182.6 2402149.8, -105183.5 2402151.3, -105184.3 2402152.9, -105184.8 2402154.6, -105185.2 2402156.3, " +
        "-105185.3 2402158.1, -105185.2 2402159.9, -105184.8 2402161.6, -105184.3 2402163.3, -105183.5 2402164.9, " +
        "-105182.6 2402166.4, -105181.4 2402167.8, -105180.1 2402169, -105178.7 2402170.1, -105177.1 2402170.9, " +
        "-105175.5 2402171.6, -105173.8 2402172, -105172 2402172.3, -105170.2 2402172.3, -105168.4 2402172, " +
        "-105166.7 2402171.6, -105165.1 2402170.9, -105163.5 2402170.1, -105162.1 2402169, -105160.8 2402167.8))"
    val cellLineWithGeom = cellLineNoGeom + s"|$shapeWkt"
    val fields = Array("420030145704439", "21.62", "39.16", "3G", "Macro", "38", "90", "15", "900", "90", shapeWkt)
    val cell =
      Cell("420030145704439", UtmCoordinates(-105171.1, 2402158.1), ThreeG, Macro, 38, 90, 15, 900, 90, shapeWkt)
  }

  "Cell" should "be built from CSV without the latest geometry field (WKT)" in new WithCell {
    CsvParser.fromLine[Cell](cellLineNoGeom).value.get should be (cell)
  }

  it should "be built from CSV with the latest geometry field (WKT)" in new WithCell {
    CsvParser.fromLine[Cell](cellLineWithGeom).value.get should be (cell)
  }

  it should "be discarded when the CSV format is wrong" in new WithCell {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(3, "5G"))
  }

  it should "be discarded when CGI length is wrong" in new WithCell {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(0, "12345678901234"))
  }

  it should "be built from CSV with a 2G cell" in new WithCell {
    fromCsv.fromFields(fields.updated(3, "2G")) should be (cell.copy(technology = TwoG))
  }

  it should "be built from CSV with a 3G cell" in new WithCell {
    fromCsv.fromFields(fields.updated(3, "3G")) should be (cell.copy(technology = ThreeG))
  }

  it should "be built from CSV with a 4G FDD cell" in new WithCell {
    fromCsv.fromFields(fields.updated(3, "4G_FDD")) should be (cell.copy(technology = FourGFdd))
  }

  it should "be built from CSV with a 4G TDD cell" in new WithCell {
    fromCsv.fromFields(fields.updated(3, "4G_TDD")) should be (cell.copy(technology = FourGTdd))
  }

  it should "be built from CSV with RDU type" in new WithCell {
    fromCsv.fromFields(fields.updated(4, "Rdu")) should be (cell.copy(cellType = Rdu))
  }

  it should "be built from CSV with CRANE type" in new WithCell {
    fromCsv.fromFields(fields.updated(4, "Crane")) should be (cell.copy(cellType = Crane))
  }

  it should "be built from CSV with MACRO type" in new WithCell {
    fromCsv.fromFields(fields.updated(4, "Macro")) should be (cell.copy(cellType = Macro))
  }

  it should "be built from CSV with RT type" in new WithCell {
    fromCsv.fromFields(fields.updated(4, "Rt")) should be (cell.copy(cellType = Rt))
  }

  it should "be built from CSV with MICRO type" in new WithCell {
    fromCsv.fromFields(fields.updated(4, "Micro")) should be (cell.copy(cellType = Micro))
  }

  it should "be built from CSV with TOWER type" in new WithCell {
    fromCsv.fromFields(fields.updated(4, "Tower")) should be (cell.copy(cellType = Tower))
  }

  it should "be built from CSV with INDOOR type" in new WithCell {
    fromCsv.fromFields(fields.updated(4, "Indoor")) should be (cell.copy(cellType = Indoor))
  }

  it should "be built from CSV with MONOPOLE type" in new WithCell {
    fromCsv.fromFields(fields.updated(4, "Monopole")) should be (cell.copy(cellType = Monopole))
  }

  it should "be built from CSV with RDS type" in new WithCell {
    fromCsv.fromFields(fields.updated(4, "Rds")) should be (cell.copy(cellType = Rds))
  }

  it should "be built from CSV with PALM TREE type" in new WithCell {
    fromCsv.fromFields(fields.updated(4, "Palm Tree")) should be (cell.copy(cellType = PalmTree))
  }

  it should "be built from CSV with OUTLET type" in new WithCell {
    fromCsv.fromFields(fields.updated(4, "Outlet")) should be (cell.copy(cellType = Outlet))
  }

  it should "be built from CSV with PARKING type" in new WithCell {
    fromCsv.fromFields(fields.updated(4, "Parking")) should be (cell.copy(cellType = Parking))
  }

  it should "provide MCC from CGI" in new WithCell {
    cell.mcc should be ("420")
  }

  it should "provide MNC from CGI" in new WithCell {
    cell.mnc should be ("030")
  }

  it should "provide LAC from CGI (for all technologies)" in new WithCell {
    cell.copy(technology = TwoG).lac should be ("1457")
    cell.copy(technology = ThreeG).lac should be ("1457")
    cell.copy(technology = FourGFdd).lac should be ("14570")
    cell.copy(technology = FourGTdd).lac should be ("14570")
  }

  it should "provide Cell Id from CGI (for all technologies)" in new WithCell {
    cell.copy(technology = TwoG).cellId should be ("04439")
    cell.copy(technology = ThreeG).cellId should be ("04439")
    cell.copy(technology = FourGFdd).cellId should be ("4439")
    cell.copy(technology = FourGTdd).cellId should be ("4439")
  }
  
  it should "provide the coverage geometry" in new WithCell with EdmCustomMatchers {
    cell.coverageGeom should equalGeometry (GeomUtils.parseWkt(shapeWkt, sridPlanar))
  }
}
