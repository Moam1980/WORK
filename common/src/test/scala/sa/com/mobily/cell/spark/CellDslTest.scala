/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell._
import sa.com.mobily.geometry.UtmCoordinates
import sa.com.mobily.utils.LocalSparkContext

class CellDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import CellDsl._

  trait WithCoords {
    val coords = UtmCoordinates(-228902.5, 3490044.0)
  }

  trait WithSqmCells extends WithCoords {
    val sqmCell1 = SqmCell("4465390", "4465390", "eNB_446539_0", "6539", "YB6539_P3_LTE", 57, coords, "HUAWEI",
      FourGTdd, Macro, 25.0, 0.0, "M1P1 and M1P2 and M1P3 and M1P4 and M1P5", "NORTH", "SECTOR", 15.2, -128, 2600)
    val sqmCell2 = SqmCell("4465390", "4465390", "eNB_446539_0", "1234", "YB6539_P3_LTE", 312, coords, "HUAWEI",
      ThreeG, Pico, 30.0, 0.0, "M1P1 and M1P2 and M1P3 and M1P4 and M1P5", "NORTH", "SECTOR", 15.2, -128, 2600)
    val sqmCell3 = SqmCell("4465391", "4465391", "eNB_446539_1", "6539", "YB6539_P3_LTE", 57, coords, "HUAWEI",
      FourGFdd, Micro, 25.0, 180.0, "M1P1 and M1P2 and M1P3 and M1P4 and M1P5", "NORTH", "SECTOR", 15.2, -128, 2600)

    val sqmCellRdd = sc.parallelize(Array(sqmCell1, sqmCell2, sqmCell3))
  }

  trait WithEgBts extends WithCoords {
    val egBts = EgBts("6539", "6539", "New-Addition", coords, "", "", 57, "BTS", "Alcatel", "E317",
      "42003000576539", TwoG, "17", "Macro", 535.49793639, 681.54282813)
    val egBtsRdd = sc.parallelize(Array(egBts))
  }

  trait WithCell extends WithCoords {
    val cell1 = Cell(
      cellId = "4465390",
      lac = 57,
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
      cellId = "4465391",
      lac = 57,
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

  "CellDsl" should "build Cell from joining SqmCell and EgBts" in new WithSqmCells with WithEgBts with WithCell {
    sqmCellRdd.toCell(egBtsRdd).count should be (2)
  }

  it should "discard SqmCell that do not match any BTS site" in new WithSqmCells with WithEgBts with WithCell {
    sc.parallelize(Array(sqmCell2)).toCell(egBtsRdd).count should be (0)
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

  it should "merge SqmCell with EgBts using BTS id and LAC" in new WithSqmCells with WithEgBts with WithCell {
    val Array(outCell1, outCell3) = sqmCellRdd.toCell(egBtsRdd).take(2)
    outCell1 should be (cell1)
    outCell3 should be (cell3)
  }
}
