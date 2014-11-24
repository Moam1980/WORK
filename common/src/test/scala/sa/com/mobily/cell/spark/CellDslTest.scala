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
      range = 681.54282813,
      coverageWkt = "POLYGON ((-228969.5 3490032.3, -228977.7 3490031.6, -229017.2 3490033.9, -229056.8 3490042.1, " +
        "-229095.7 3490056.2, -229132.9 3490076.1, -229167.6 3490101.5, -229199 3490132.2, -229226.2 3490167.5, " +
        "-229248.7 3490206.9, -229265.7 3490249.7, -229277 3490295.1, -229281.9 3490342.2, -229280.5 3490390.1, " +
        "-229272.4 3490438, -229257.9 3490484.8, -229237 3490529.6, -229210.1 3490571.6, -229177.5 3490609.9, " +
        "-229139.9 3490643.7, -229098 3490672.3, -229052.4 3490695.3, -229004 3490712, -228953.7 3490722.1, " +
        "-228902.5 3490725.5, -228851.3 3490722.1, -228801 3490712, -228752.6 3490695.3, -228707 3490672.3, " +
        "-228665.1 3490643.7, -228627.5 3490609.9, -228594.9 3490571.6, -228568 3490529.6, -228547.1 3490484.8, " +
        "-228532.6 3490438, -228524.5 3490390.1, -228523.1 3490342.2, -228528 3490295.1, -228539.3 3490249.7, " +
        "-228556.3 3490206.9, -228578.8 3490167.5, -228606 3490132.2, -228637.4 3490101.5, -228672.1 3490076.1, " +
        "-228709.3 3490056.2, -228748.2 3490042.1, -228787.8 3490033.9, -228827.3 3490031.6, -228835.5 3490032.3, " +
        "-228836.5 3490027.1, -228839.1 3490018.9, -228842.8 3490011.2, -228847.4 3490003.9, -228852.8 3489997.3, " +
        "-228859.1 3489991.5, -228866 3489986.5, -228873.5 3489982.3, -228881.4 3489979.2, -228889.7 3489977.1, " +
        "-228898.2 3489976, -228906.8 3489976, -228915.3 3489977.1, -228923.6 3489979.2, -228931.5 3489982.3, " +
        "-228939 3489986.5, -228945.9 3489991.5, -228952.2 3489997.3, -228957.6 3490003.9, -228962.2 3490011.2, " +
        "-228965.9 3490018.9, -228968.5 3490027.1, -228969.5 3490032.3))",
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
      range = 681.54282813,
      coverageWkt = "POLYGON ((-228835.5 3490055.7, -228827.3 3490056.4, -228787.8 3490054.1, -228748.2 3490045.9, " +
        "-228709.3 3490031.8, -228672.1 3490011.9, -228637.4 3489986.5, -228606 3489955.8, -228578.8 3489920.5, " +
        "-228556.3 3489881.1, -228539.3 3489838.3, -228528 3489792.9, -228523.1 3489745.8, -228524.5 3489697.9, " +
        "-228532.6 3489650, -228547.1 3489603.2, -228568 3489558.4, -228594.9 3489516.4, -228627.5 3489478.1, " +
        "-228665.1 3489444.3, -228707 3489415.7, -228752.6 3489392.7, -228801 3489376, -228851.3 3489365.9, " +
        "-228902.5 3489362.5, -228953.7 3489365.9, -229004 3489376, -229052.4 3489392.7, -229098 3489415.7, " +
        "-229139.9 3489444.3, -229177.5 3489478.1, -229210.1 3489516.4, -229237 3489558.4, -229257.9 3489603.2, " +
        "-229272.4 3489650, -229280.5 3489697.9, -229281.9 3489745.8, -229277 3489792.9, -229265.7 3489838.3, " +
        "-229248.7 3489881.1, -229226.2 3489920.5, -229199 3489955.8, -229167.6 3489986.5, -229132.9 3490011.9, " +
        "-229095.7 3490031.8, -229056.8 3490045.9, -229017.2 3490054.1, -228977.7 3490056.4, -228969.5 3490055.7, " +
        "-228968.5 3490060.9, -228965.9 3490069.1, -228962.2 3490076.8, -228957.6 3490084.1, -228952.2 3490090.7, " +
        "-228945.9 3490096.5, -228939 3490101.5, -228931.5 3490105.7, -228923.6 3490108.8, -228915.3 3490110.9, " +
        "-228906.8 3490112, -228898.2 3490112, -228889.7 3490110.9, -228881.4 3490108.8, -228873.5 3490105.7, " +
        "-228866 3490101.5, -228859.1 3490096.5, -228852.8 3490090.7, -228847.4 3490084.1, -228842.8 3490076.8, " +
        "-228839.1 3490069.1, -228836.5 3490060.9, -228835.5 3490055.7))",
      mcc = "420",
      mnc = "03")
  }

  trait WithCellsText {

    val cellText1 = "420|03|28671|3328|1006392.2|2937305.5|EPSG:32638|3G|MACRO|28.0|70.0|204.0|801.81509192|" +
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
    val cellText2 = "420|03|40441|4526|126371.9|3026378.9|EPSG:32638|2G|MACRO|60.0|100.0|228.0|2525.71753955|" +
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
    val cellText3 = "420|03|NonValidCell|4526|126371.9|3026378.9|EPSG:32638|2G|MACRO|60.0|100.0|228.0|2525.71753955|" +
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
      coverageWkt = "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))")
    val cell2 = cell1.copy(cellId = 4465391, coverageWkt = "POLYGON ((2 0, 2 2, 5 2, 5 0, 2 0))")
    val cell3 = cell1.copy(cellId = 4465392, coverageWkt = "POLYGON ((7 7, 7 9, 9 9, 9 7, 7 7))")
    val cells = sc.parallelize(Array(cell1, cell2, cell3))

    val location = GeomUtils.parseWkt("POLYGON ((1 0, 1 2, 3 2, 3 0, 1 0))", Coordinates.SaudiArabiaUtmSrid)

    val cellMetrics1 =
      LocationCellMetrics(
        cellIdentifier = (57, 4465390),
        cellWkt = "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))",
        centroidDistance = 1,
        areaRatio = 1)
    val cellMetrics2 =
      LocationCellMetrics(
        cellIdentifier = (57, 4465391),
        cellWkt = "POLYGON ((2 0, 2 2, 5 2, 5 0, 2 0))",
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
