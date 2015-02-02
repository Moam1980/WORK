/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell._
import sa.com.mobily.geometry.{Coordinates, GeomUtils, UtmCoordinates}
import sa.com.mobily.utils.{EdmCustomMatchers, LocalSparkContext}

class CellDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext with EdmCustomMatchers {

  import CellDsl._

  trait WithCoords {
    val coords = UtmCoordinates(-228902.5, 3490044.0)
  }

  trait WithSqmCells extends WithCoords {
    val sqmCell1 = SqmCell(4465390, "4465390", "eNB_446539_0", "6539", "YB6539_P3_LTE", 57, coords, "HUAWEI",
      FourGTdd, Macro, 25.0, 0.0, "M1P1 and M1P2 and M1P3 and M1P4 and M1P5", "NORTH", "SECTOR", 15.2, -128, 2600)
    val sqmCell2 = SqmCell(4465390, "4465390", "eNB_446539_0", "1234", "YB6539_P3_LTE", 312, coords, "HUAWEI",
      ThreeG, Pico, 30.0, 0.0, "M1P1 and M1P2 and M1P3 and M1P4 and M1P5", "NORTH", "SECTOR", 15.2, -128, 2600)
    val sqmCell3 = SqmCell(4465391, "4465391", "eNB_446539_1", "6539", "YB6539_P3_LTE", 57, coords, "HUAWEI",
      FourGFdd, Micro, 25.0, 180.0, "M1P1 and M1P2 and M1P3 and M1P4 and M1P5", "NORTH", "SECTOR", 15.2, -128, 2600)

    val sqmCellRdd = sc.parallelize(Array(sqmCell1, sqmCell2, sqmCell3))
  }

  trait WithEgBts extends WithCoords {
    val egBts = EgBts("6539", "6539", "New-Addition", coords, "", "", 57, "BTS", "Alcatel", "E317",
      "42003000576539", TwoG, "17", "Macro", 535.49793639, 681.54282813)
    val egBtsRdd = sc.parallelize(Array(egBts))
  }

  trait WithCells extends WithCoords {
    val cell1 = Cell(
      cellId = 4465390,
      lacTac = 57,
      planarCoords = coords,
      technology = FourGTdd,
      cellType = Macro,
      height = 25.0,
      azimuth = 0.0,
      beamwidth = 216,
      range = 1022.3142421949999,
      bts = "6539",
      coverageWkt = "POLYGON ((-229003 3490026.5, -229015.3 3490025.4, -229074.6 3490028.8, -229134 3490041.1, " +
        "-229192.3 3490062.2, -229248.1 3490092.1, -229300.2 3490130.3, -229347.2 3490176.2, -229388.1 3490229.2, " +
        "-229421.7 3490288.3, -229447.4 3490352.5, -229464.2 3490420.6, -229471.7 3490491.3, -229469.5 3490563.2, " +
        "-229457.4 3490634.9, -229435.6 3490705.1, -229404.2 3490772.4, -229363.8 3490835.4, -229315 3490892.8, " +
        "-229258.6 3490943.5, -229195.7 3490986.5, -229127.3 3491020.9, -229054.7 3491046, -228979.3 3491061.2, " +
        "-228902.5 3491066.3, -228825.7 3491061.2, -228750.3 3491046, -228677.7 3491020.9, -228609.3 3490986.5, " +
        "-228546.4 3490943.5, -228490 3490892.8, -228441.2 3490835.4, -228400.8 3490772.4, -228369.4 3490705.1, " +
        "-228347.6 3490634.9, -228335.5 3490563.2, -228333.3 3490491.3, -228340.8 3490420.6, -228357.6 3490352.5, " +
        "-228383.3 3490288.3, -228416.9 3490229.2, -228457.8 3490176.2, -228504.8 3490130.3, -228556.9 3490092.1, " +
        "-228612.7 3490062.2, -228671 3490041.1, -228730.4 3490028.8, -228789.7 3490025.4, -228802 3490026.5, " +
        "-228803.5 3490018.6, -228807.4 3490006.4, -228812.9 3489994.7, -228819.8 3489983.9, -228828 3489974, " +
        "-228837.3 3489965.2, -228847.7 3489957.7, -228859 3489951.5, -228870.9 3489946.8, -228883.3 3489943.6, " +
        "-228896.1 3489942, -228908.9 3489942, -228921.7 3489943.6, -228934.1 3489946.8, -228946 3489951.5, " +
        "-228957.3 3489957.7, -228967.7 3489965.2, -228977 3489974, -228985.2 3489983.9, -228992.1 3489994.7, " +
        "-228997.6 3490006.4, -229001.5 3490018.6, -229003 3490026.5))",
      mcc = "420",
      mnc = "03")
    val cell3 = Cell(
      cellId = 4465391,
      lacTac = 57,
      planarCoords = coords,
      technology = FourGFdd,
      cellType = Micro,
      height = 25.0,
      azimuth = 180.0,
      beamwidth = 216,
      range = 1022.3142421949999,
      bts = "6539",
      coverageWkt = "POLYGON ((-228802 3490061.5, -228789.7 3490062.6, -228730.4 3490059.2, -228671 3490046.9, " +
        "-228612.7 3490025.8, -228556.9 3489995.9, -228504.8 3489957.7, -228457.8 3489911.8, -228416.9 3489858.8, " +
        "-228383.3 3489799.7, -228357.6 3489735.5, -228340.8 3489667.4, -228333.3 3489596.7, -228335.5 3489524.8, " +
        "-228347.6 3489453.1, -228369.4 3489382.9, -228400.8 3489315.6, -228441.2 3489252.6, -228490 3489195.2, " +
        "-228546.4 3489144.5, -228609.3 3489101.5, -228677.7 3489067.1, -228750.3 3489042, -228825.7 3489026.8, " +
        "-228902.5 3489021.7, -228979.3 3489026.8, -229054.7 3489042, -229127.3 3489067.1, -229195.7 3489101.5, " +
        "-229258.6 3489144.5, -229315 3489195.2, -229363.8 3489252.6, -229404.2 3489315.6, -229435.6 3489382.9, " +
        "-229457.4 3489453.1, -229469.5 3489524.8, -229471.7 3489596.7, -229464.2 3489667.4, -229447.4 3489735.5, " +
        "-229421.7 3489799.7, -229388.1 3489858.8, -229347.2 3489911.8, -229300.2 3489957.7, -229248.1 3489995.9, " +
        "-229192.3 3490025.8, -229134 3490046.9, -229074.6 3490059.2, -229015.3 3490062.6, -229003 3490061.5, " +
        "-229001.5 3490069.4, -228997.6 3490081.6, -228992.1 3490093.3, -228985.2 3490104.1, -228977 3490114, " +
        "-228967.7 3490122.8, -228957.3 3490130.3, -228946 3490136.5, -228934.1 3490141.2, -228921.7 3490144.4, " +
        "-228908.9 3490146, -228896.1 3490146, -228883.3 3490144.4, -228870.9 3490141.2, -228859 3490136.5, " +
        "-228847.7 3490130.3, -228837.3 3490122.8, -228828 3490114, -228819.8 3490104.1, -228812.9 3490093.3, " +
        "-228807.4 3490081.6, -228803.5 3490069.4, -228802 3490061.5))",
      mcc = "420",
      mnc = "03")
  }

  trait WithCellsText {

    val cellText1 = "420|03|28671|3328|1006392.2|2937305.5|EPSG:32638|3G|MACRO|28.0|70.0|204.0|801.81509192|6539|" +
      "POLYGON ((1006359 2937378.4, 1006354.4 2937391.1, 1006344.6 2937438, 1006341.4 2937486.5, 1006345 2937535.7, " +
      "1006355.4 2937584.5, 1006372.6 2937632.1, 1006396.4 2937677.5, 1006426.4 2937719.7, 1006462.2 2937757.9, " +
      "1006503.3 2937791.3, 1006548.9 2937819.3, 1006598.2 2937841.1, 1006650.5 2937856.3, 1006704.7 2937864.6, " +
      "1006759.9 2937865.7, 1006815.1 2937859.5, 1006869.4 2937846, 1006921.7 2937825.5, 1006971.1 2937798.2, " +
      "1007016.7 2937764.6, 1007057.6 2937725.2, 1007093.2 2937680.7, 1007122.7 2937632, 1007145.7 2937579.7, " +
      "1007161.6 2937525, 1007170.4 2937468.6, 1007171.7 2937411.7, 1007165.7 2937355.2, 1007152.4 2937300.2, " +
      "1007132.1 2937247.5, 1007105.2 2937198.2, 1007072.3 2937153, 1007034 2937112.7, 1006991 2937078.1, " +
      "1006944.1 2937049.5, 1006894.3 2937027.6, 1006842.5 2937012.6, 1006789.6 2937004.7, 1006736.7 2937004, " +
      "1006684.6 2937010.2, 1006634.5 2937023.2, 1006587.1 2937042.7, 1006543.3 2937068.1, 1006504 2937098.8, " +
      "1006469.6 2937134.2, 1006440.9 2937173.4, 1006418.2 2937215.6, 1006413.5 2937228.3, 1006407.2 2937226.7, " +
      "1006397.2 2937225.5, 1006387.2 2937225.5, 1006377.2 2937226.7, 1006367.4 2937229.2, 1006358.1 2937232.9, " +
      "1006349.2 2937237.8, 1006341.1 2937243.7, 1006333.8 2937250.6, 1006327.3 2937258.4, 1006321.9 2937266.9, " +
      "1006317.6 2937276, 1006314.5 2937285.6, 1006312.7 2937295.5, 1006312 2937305.5, 1006312.7 2937315.5, " +
      "1006314.5 2937325.4, 1006317.6 2937335, 1006321.9 2937344.1, 1006327.3 2937352.6, 1006333.8 2937360.4, " +
      "1006341.1 2937367.3, 1006349.2 2937373.2, 1006358.1 2937378.1, 1006359 2937378.4))|" +
      "Unused"
    val cellText2 = "420|03|40441|4526|126371.9|3026378.9|EPSG:32638|2G|MACRO|60.0|100.0|228.0|2525.71753955|1234|" +
      "POLYGON ((126349.1 3026630.1, 126349 3026647.1, 126369.6 3026787.5, 126412.6 3026927.5, 126477.9 3027063.4, " +
      "126565 3027191.8, 126672.7 3027309.1, 126799.5 3027412.2, 126943.1 3027498, 127100.9 3027563.8, " +
      "127269.9 3027607.6, 127446.6 3027627.5, 127627.6 3027622.4, 127809.1 3027591.7, 127987.1 3027535.3, " +
      "128157.9 3027453.7, 128317.8 3027348.2, 128463.2 3027220.4, 128590.9 3027072.6, 128698 3026907.4, " +
      "128782.3 3026728, 128841.6 3026537.7, 128874.6 3026340.5, 128880.6 3026140, 128859.2 3025940.3, " +
      "128811 3025745.4, 128736.8 3025559, 128638.3 3025385, 128517.5 3025226.5, 128377 3025086.7, " +
      "128219.8 3024968.1, 128049.2 3024872.9, 127868.9 3024802.5, 127682.6 3024758, 127494.2 3024739.8, " +
      "127307.6 3024747.7, 127126.6 3024780.9, 126954.8 3024838, 126795.5 3024917.2, 126651.6 3025016.1, " +
      "126525.9 3025132, 126420.3 3025261.7, 126336.4 3025401.9, 126275.3 3025549, 126237.4 3025699.4, " +
      "126222.5 3025849.5, 126230 3025995.7, 126258.6 3026134.7, 126264.5 3026150.4, 126264.4 3026150.4, " +
      "126236.6 3026165.6, 126210.9 3026184.3, 126187.8 3026206, 126167.6 3026230.4, 126150.6 3026257.2, " +
      "126137.1 3026285.9, 126127.3 3026316.1, 126121.3 3026347.2, 126119.3 3026378.9, 126121.3 3026410.6, " +
      "126127.3 3026441.7, 126137.1 3026471.9, 126150.6 3026500.6, 126167.6 3026527.4, 126187.8 3026551.8, " +
      "126210.9 3026573.5, 126236.6 3026592.2, 126264.4 3026607.4, 126293.9 3026619.1, 126324.6 3026627, " +
      "126349.1 3026630.1))|" +
      "Unused"
    val cellText3 = "420|03|NonValidCell|4526|126371.9|3026378.9|EPSG:32638|2G|MACRO|60.0|100.0|228.0|" +
      "2525.71753955|6539|" +
      "POLYGON ((126349.1 3026630.1, 126349 3026647.1, 126369.6 3026787.5, 126412.6 3026927.5, 126477.9 3027063.4, " +
      "126565 3027191.8, 126672.7 3027309.1, 126799.5 3027412.2, 126943.1 3027498, 127100.9 3027563.8, " +
      "127269.9 3027607.6, 127446.6 3027627.5, 127627.6 3027622.4, 127809.1 3027591.7, 127987.1 3027535.3, " +
      "128157.9 3027453.7, 128317.8 3027348.2, 128463.2 3027220.4, 128590.9 3027072.6, 128698 3026907.4, " +
      "128782.3 3026728, 128841.6 3026537.7, 128874.6 3026340.5, 128880.6 3026140, 128859.2 3025940.3, " +
      "128811 3025745.4, 128736.8 3025559, 128638.3 3025385, 128517.5 3025226.5, 128377 3025086.7, " +
      "128219.8 3024968.1, 128049.2 3024872.9, 127868.9 3024802.5, 127682.6 3024758, 127494.2 3024739.8, " +
      "127307.6 3024747.7, 127126.6 3024780.9, 126954.8 3024838, 126795.5 3024917.2, 126651.6 3025016.1, " +
      "126525.9 3025132, 126420.3 3025261.7, 126336.4 3025401.9, 126275.3 3025549, 126237.4 3025699.4, " +
      "126222.5 3025849.5, 126230 3025995.7, 126258.6 3026134.7, 126264.5 3026150.4, 126264.4 3026150.4, " +
      "126236.6 3026165.6, 126210.9 3026184.3, 126187.8 3026206, 126167.6 3026230.4, 126150.6 3026257.2, " +
      "126137.1 3026285.9, 126127.3 3026316.1, 126121.3 3026347.2, 126119.3 3026378.9, 126121.3 3026410.6, " +
      "126127.3 3026441.7, 126137.1 3026471.9, 126150.6 3026500.6, 126167.6 3026527.4, 126187.8 3026551.8, " +
      "126210.9 3026573.5, 126236.6 3026592.2, 126264.4 3026607.4, 126293.9 3026619.1, 126324.6 3026627, " +
      "126349.1 3026630.1))|" +
      "Unused"

    val cellsText = sc.parallelize(Array(cellText1, cellText2, cellText3))
  }

  trait WithCellsAndLacs {

    val cell1 = Cell(
      cellId = 4465390,
      lacTac = 57,
      planarCoords = UtmCoordinates(0, 0),
      technology = FourGTdd,
      cellType = Macro,
      height = 25.0,
      azimuth = 0.0,
      beamwidth = 216,
      range = 681.54282813,
      bts = "6539",
      coverageWkt = "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))")
    val cell2 = cell1.copy(coverageWkt = "POLYGON ((0.5 0, 0.5 1, 1.5 1, 1.5 0, 0.5 0))")
    val aggGeom = GeomUtils.parseWkt("POLYGON ((0 0, 0 1, 1.5 1, 1.5 0, 0 0))", Coordinates.SaudiArabiaUtmSrid)

    val cells = sc.parallelize(Array(cell1, cell2))
  }

  trait WithCellsAndLocations {

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
      bts = "6539",
      coverageWkt = "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))")
    val cell2 = cell1.copy(cellId = 4465391, coverageWkt = "POLYGON ((2 0, 2 2, 5 2, 5 0, 2 0))")
    val cell3 = cell1.copy(cellId = 4465392, coverageWkt = "POLYGON ((7 7, 7 9, 9 9, 9 7, 7 7))")
    val cells = sc.parallelize(Array(cell1, cell2, cell3))

    val location = GeomUtils.parseWkt("POLYGON ((1 0, 1 2, 3 2, 3 0, 1 0))", Coordinates.SaudiArabiaUtmSrid)

    val cellMetrics1 =
      LocationCellMetrics(
        cellIdentifier = (57, 4465390),
        cellWkt = "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))",
        cellArea = 4,
        technology = "4G_TDD",
        cellType = "MACRO",
        range = 2530.3,
        centroidDistance = 1,
        areaRatio = 1)
    val cellMetrics2 =
      LocationCellMetrics(
        cellIdentifier = (57, 4465391),
        cellWkt = "POLYGON ((2 0, 2 2, 5 2, 5 0, 2 0))",
        cellArea = 6,
        technology = "4G_TDD",
        cellType = "MACRO",
        range = 2530.3,
        centroidDistance = 1.5,
        areaRatio = 1.5)
    val aggMetrics =
      LocationCellAggMetrics(
        cellsWkt = List("POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))", "POLYGON ((2 0, 2 2, 5 2, 5 0, 2 0))"),
        numberOfCells = 2,
        centroidDistanceAvg = 1.25,
        centroidDistanceStDev = 0.25,
        centroidDistanceMin = 1,
        centroidDistanceMax = 1.5,
        areaRatioAvg = 1.25,
        areaRatioStDev = 0.25)
  }

  "CellDsl" should "build Cell from joining SqmCell and EgBts" in new WithSqmCells with WithEgBts with WithCells {
    sqmCellRdd.toCell(egBtsRdd).count should be (2)
  }

  it should "discard SqmCell that do not match any BTS site" in new WithSqmCells with WithEgBts with WithCells {
    sc.parallelize(Array(sqmCell2)).toCell(egBtsRdd).count should be (0)
  }

  it should "broadcast the cells with (LAC, CellId) as key" in new WithCells {
    val cells = sc.parallelize(Array(cell1, cell3))
    val broadcastMap = cells.toBroadcastMap
    broadcastMap.value.size should be (2)
    broadcastMap.value((57, 4465390)) should be (cell1)
    broadcastMap.value((57, 4465391)) should be (cell3)
  }

  it should "get correctly parsed cells" in new WithCellsText {
    cellsText.toCell.count should be (2)
  }

  it should "get errors when parsing cells" in new WithCellsText {
    cellsText.toCellErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed cells" in new WithCellsText {
    cellsText.toParsedCell.count should be (3)
  }

  "CellMerger" should "compute beamwidths when there are duplicates" in {
    val azimuthsWithDuplicates = List[Double](45, 20, 180, 180, 20, 180, 235)
    val beamwidths = Map[Double, Double](45d -> 162d, 20d -> 174d, 180d -> 162d, 235d -> 174d)
    CellMerger.computeBeamwidths(azimuthsWithDuplicates) should be (beamwidths)
  }

  it should "compute beamwidth (rounded to 360) when there is only one azimuth" in {
    val singleAzimuth = List[Double](35)
    val beamwidth = Map[Double, Double](35d -> 360d)
    CellMerger.computeBeamwidths(singleAzimuth) should be (beamwidth)
  }

  it should "compute beamwidth when azimuths are very close from each other and pointing to Northern positions" in {
    val northernAzimuths = List[Double](310, 20)
    val beamwidths = Map[Double, Double](310d -> 348d, 20d -> 348d)
    CellMerger.computeBeamwidths(northernAzimuths) should be (beamwidths)
  }

  it should "merge SqmCell with EgBts using BTS id and LAC" in new WithSqmCells with WithEgBts with WithCells {
    val Array(outCell1, outCell3) = sqmCellRdd.toCell(egBtsRdd).take(2)
    outCell1 should be (cell1)
    outCell3 should be (cell3)
  }

  it should "build LAC geometries" in new WithCellsAndLacs {
    val lacGeoms = cells.lacGeometries
    lacGeoms.count should be (1)
    lacGeoms.first._1 should be (57)
    lacGeoms.first._2 should equalGeometry (aggGeom)
  }

  it should "generate location cell metrics" in new WithCellsAndLocations {
    val metrics = cells.locationCellMetrics(location).collect.toList
    metrics.size should be (2)
    metrics should contain (cellMetrics1)
    metrics should contain (cellMetrics2)
  }

  it should "generate aggregated location cell metrics" in new WithCellsAndLocations {
    cells.locationCellAggMetrics(location) should be (aggMetrics)
  }
}
