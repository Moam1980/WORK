/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.geometry.{Coordinates, GeomUtils, UtmCoordinates}
import sa.com.mobily.parsing.CsvParser
import sa.com.mobily.utils.EdmCustomMatchers

class CellTest extends FlatSpec with ShouldMatchers {

  import Cell._

  trait WithCell {

    val sridPlanar = 32638
    val shapeWkt = "POLYGON ((684233.4 2749404.5, 684232.3 2749404.8, 684182.5 2749426.3, " +
      "684134.8 2749456.6, 684090.7 2749495.4, 684051.6 2749542.1, 684018.9 2749596, 683993.8 2749656, " +
      "683977.3 2749721, 683970.4 2749789.6, 683973.6 2749860.3, 683987.2 2749931.4, 684011.4 2750001.3, " +
      "684046.1 2750068.3, 684090.7 2750130.9, 684144.6 2750187.4, 684206.8 2750236.4, 684276.2 2750276.8, " +
      "684351.4 2750307.3, 684430.8 2750327.2, 684512.9 2750335.9, 684596 2750333, 684678.2 2750318.5, " +
      "684757.8 2750292.7, 684833.2 2750255.9, 684902.8 2750209, 684965 2750153, 685018.7 2750089, " +
      "685062.7 2750018.5, 685096.2 2749943.1, 685118.7 2749864.3, 685129.8 2749783.9, 685129.6 2749703.7, " +
      "685118.2 2749625.2, 685096.2 2749550.3, 685064.4 2749480.4, 685023.6 2749416.9, 684975.2 2749360.9, " +
      "684920.4 2749313.6, 684860.9 2749275.5, 684798 2749247.2, 684733.5 2749229, 684668.9 2749220.7, " +
      "684605.9 2749222.1, 684545.9 2749232.6, 684490.2 2749251.4, 684440.1 2749277.6, 684396.6 2749309.9, " +
      "684395.9 2749310.6, 684385.8 2749303.3, 684374.8 2749297.2, 684363.1 2749292.6, 684350.9 2749289.4, " +
      "684338.4 2749287.9, 684325.8 2749287.9, 684313.3 2749289.4, 684301.1 2749292.6, 684289.4 2749297.2, " +
      "684278.4 2749303.3, 684268.2 2749310.7, 684259 2749319.3, 684251 2749329, 684244.3 2749339.6, " +
      "684238.9 2749351, 684235 2749363, 684232.7 2749375.3, 684231.9 2749387.9, 684232.7 2749400.5, " +
      "684233.4 2749404.5))"

    val shapeWgs84Wkt = "POLYGON ((46.82329508 24.8484987, 46.82328424 24.84850154, " +
      "46.82279442 24.84870162, 46.82232655 24.84898089, 46.82189541 24.84933645, 46.82151477 24.84976271, " +
      "46.8211984 24.85025319, 46.82095802 24.85079782, 46.82080338 24.85138655, 46.82074419 24.85200661, " +
      "46.82078519 24.85264441, 46.82092913 24.85328457, 46.82117779 24.85391262, 46.82152994 24.85451322, " +
      "46.82197946 24.85507291, 46.82252018 24.85557641, 46.82314203 24.8560112, 46.82383397 24.8563675, " +
      "46.82458199 24.85663372, 46.82537016 24.85680375, 46.82618356 24.85687236, 46.82700531 24.85683612, " +
      "46.82781662 24.85669529, 46.82860071 24.85645276, 46.82934177 24.85611144, 46.83002412 24.85567966, " +
      "46.83063203 24.85516662, 46.83115479 24.8545824, 46.83158071 24.85394069, 46.8319021 24.85325602, " +
      "46.83211422 24.85254199, 46.83221334 24.8518149, 46.83220069 24.85109099, 46.83207747 24.85038378, " +
      "46.83184987 24.84971035, 46.83152599 24.84908325, 46.83111393 24.84851501, 46.83062768 24.84801538, " +
      "46.83007928 24.84759506, 46.82948561 24.84725836, 46.82885961 24.84701052, 46.82821913 24.84685405, " +
      "46.82757897 24.84678695, 46.82695592 24.84680721, 46.82636376 24.84690925, 46.82581524 24.84708568, " +
      "46.82532309 24.84732824, 46.82489704 24.84762506, 46.82489021 24.84763146, 46.82478932 24.84756679, " +
      "46.8246797 24.84751305, 46.82456334 24.84747294, 46.82444223 24.84744553, 46.82431837 24.8474335, " +
      "46.82419373 24.84743503, 46.82407027 24.84745008, 46.82395 24.84748044, 46.82383487 24.84752337, " +
      "46.82372685 24.84757976, 46.82362693 24.84764779, 46.82353705 24.84772653, 46.8234592 24.84781506, " +
      "46.82339432 24.84791155, 46.82334241 24.84801511, 46.82330541 24.8481239, 46.82328429 24.8482352, " +
      "46.82327804 24.84834904, 46.82328762 24.84846267, 46.82329508 24.8484987))"

    val cellLine = "420|03|4465390|57|-194243.4|2671697.6|EPSG:32638|4G_TDD|MACRO|25.0|0.0|90.0|2530.3|1|" +
      shapeWkt + "|" + shapeWgs84Wkt
    val fields = Array("420", "03", "4465390", "57", "-194243.4", "2671697.6", "EPSG:32638", "4G_TDD", "MACRO", "25.0",
      "0.0", "90.0", "2530.3", "1", shapeWkt, shapeWgs84Wkt)
    val cell = Cell(
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
      bts = "1",
      coverageWkt = shapeWkt)
  }

  trait WithLocation {

    val cell = Cell(
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
      bts = "1",
      coverageWkt = "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))")
    val location = GeomUtils.parseWkt("POLYGON ((4 0, 4 2, 6 2, 6 0, 4 0))", Coordinates.SaudiArabiaUtmSrid)
  }

  trait WithIntersectedCells {

    val cell1 = Cell(
      mcc = "420",
      mnc = "03",
      cellId = 4465390,
      lacTac = 57,
      planarCoords = UtmCoordinates(0, 0, "EPSG:32638"),
      technology = FourGTdd,
      cellType = Macro,
      height = 25,
      azimuth = 0,
      beamwidth = 90,
      range = 2530.3,
      bts = "1",
      coverageWkt = "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))")
    val cell2 = cell1.copy(
      planarCoords = UtmCoordinates(1, 1, "EPSG:32638"),
      coverageWkt = "POLYGON ((1 1, 1 3, 3 3, 3 1, 1 1))")
    val cell3 = cell1.copy(
      planarCoords = UtmCoordinates(1, 1, "EPSG:32638"),
      coverageWkt = "POLYGON ((10 10, 10 30, 30 30, 30 10, 10 10))")
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
    cell.fields should be (fields)
  }

  it should "provide the cell identifier" in new WithCell {
    cell.identifier should be ((57, 4465390))
  }

  it should "compute the distance from the centroid to another geometry's centroid" in new WithLocation {
    cell.centroidDistance(location) should be (4)
  }

  it should "compute the ratio of the cell area against another location" in new WithLocation {
    cell.areaRatio(location) should be (1)
  }

  it should "detect intersections" in new WithIntersectedCells {
    cell1.intersects(cell2) should be (true)
    cell1.intersects(cell3) should be (false)
  }

  it should "read cell identifiers in tuples when field is empty" in {
    Cell.parseCellTuples("") should be (Seq())
  }

  it should "read cell identifiers in tuples for several cells" in {
    Cell.parseCellTuples("(1,3);(4,5)") should be (Seq((1, 3), (4, 5)))
  }

  it should "fail with exception when format is wrong" in {
    an [Exception] should be thrownBy Cell.parseCellTuples("(13);(4,5)")
  }
}
