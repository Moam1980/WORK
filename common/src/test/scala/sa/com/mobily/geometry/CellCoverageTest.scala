/*
 * TODO: License goes here!
 */

package sa.com.mobily.geometry

import com.vividsolutions.jts.geom.Coordinate
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell._
import sa.com.mobily.utils.EdmCustomMatchers

class CellCoverageTest extends FlatSpec with ShouldMatchers with EdmCustomMatchers {

  trait WithCellCoverageShapes {

    val sridPlanar = 32638
    val geomFactory = GeomUtils.geomFactory(sridPlanar)

    val position = geomFactory.createPoint(new Coordinate(0, 0))
    val azimuthCirSect = 135
    val azimuthHippopede = 225
    val azimuthConchoid = 315
    val beamwidthCirSect = 90
    val beamwidthLt180 = 90
    val beamwidthGe180 = 225
    val tech = FourGFdd
    val numPoints = 25

    val height = 48
    val tilt = 6
    val rangeTolerance = 1e-2
    val rangeTwoG = 639.37
    val rangeThreeGFourGFdd = 456.69
    val rangeFourGTdd = 365.35

    val expectedCircle = GeomUtils.parseWkt(
      "POLYGON ((456.7 0, 453.1 57.2, 442.3 113.6, 424.6 168.1, 400.2 220, 369.5 268.4, 332.9 312.6, 291.1 351.9," +
        " 244.7 385.6, 194.4 413.2, 141.1 434.3, 85.6 448.6, 28.7 455.8, -28.7 455.8, -85.6 448.6, -141.1 434.3, " +
        "-194.4 413.2, -244.7 385.6, -291.1 351.9, -332.9 312.6, -369.5 268.4, -400.2 220, -424.6 168.1, " +
        "-442.3 113.6, -453.1 57.2, -456.7 0, -453.1 -57.2, -442.3 -113.6, -424.6 -168.1, -400.2 -220, -369.5 -268.4," +
        " -332.9 -312.6, -291.1 -351.9, -244.7 -385.6, -194.4 -413.2, -141.1 -434.3, -85.6 -448.6, -28.7 -455.8, " +
        "28.7 -455.8, 85.6 -448.6, 141.1 -434.3, 194.4 -413.2, 244.7 -385.6, 291.1 -351.9, 332.9 -312.6, " +
        "369.5 -268.4, 400.2 -220, 424.6 -168.1, 442.3 -113.6, 453.1 -57.2, 456.7 0))",
      sridPlanar)
    val expectedCirSect = GeomUtils.parseWkt(
      "POLYGON ((45.7 0, 456.7 0, 456.5 -14.6, 455.8 -29.3, 454.6 -43.9, 452.9 -58.4, 450.8 -72.9, 448.3 -87.3, " +
        "445.2 -101.6, 441.8 -115.8, 437.8 -129.9, 433.4 -143.9, 428.6 -157.7, 423.3 -171.4, 417.6 -184.9, " +
        "411.5 -198.2, 404.9 -211.2, 397.9 -224.1, 390.5 -236.7, 382.7 -249.1, 374.6 -261.3, 366 -273.2, " +
        "357.1 -284.7, 347.7 -296, 338.1 -307, 328.1 -317.7, 317.7 -328.1, 307 -338.1, 296 -347.7, 284.7 -357.1, " +
        "273.2 -366, 261.3 -374.6, 249.1 -382.7, 236.7 -390.5, 224.1 -397.9, 211.2 -404.9, 198.2 -411.5, " +
        "184.9 -417.6, 171.4 -423.3, 157.7 -428.6, 143.9 -433.4, 129.9 -437.8, 115.8 -441.8, 101.6 -445.2, " +
        "87.3 -448.3, 72.9 -450.8, 58.4 -452.9, 43.9 -454.6, 29.3 -455.8, 14.6 -456.5, 0 -456.7, 0 -45.6, " +
        "-2.9 -45.6, -8.6 -44.9, -14.1 -43.4, -19.4 -41.3, -24.5 -38.6, -29.1 -35.2, -33.3 -31.3, -36.9 -26.8, " +
        "-40 -22, -42.5 -16.8, -44.2 -11.4, -45.3 -5.7, -45.7 0, -45.3 5.7, -44.2 11.4, -42.5 16.8, -40 22, " +
        "-36.9 26.8, -33.3 31.3, -29.1 35.2, -24.5 38.6, -19.4 41.3, -14.1 43.4, -8.6 44.9, -2.9 45.6, 2.9 45.6, " +
        "8.6 44.9, 14.1 43.4, 19.4 41.3, 24.5 38.6, 29.1 35.2, 33.3 31.3, 36.9 26.8, 40 22, 42.5 16.8, 44.2 11.4, " +
        "45.3 5.7, 45.7 0))",
      sridPlanar)
    val expectedHippopedeWithBackLobe = GeomUtils.parseWkt(
      "POLYGON ((-1.4 -45.6, -3.6 -114.4, -10.2 -161.4, -18.6 -196.8, -28.5 -225.9, -39.7 -250.7, -51.9 -272.2, " +
        "-65 -290.8, -78.8 -307, -93.3 -321, -108.2 -333, -123.5 -343.1, -139.1 -351.3, -154.9 -357.9, " +
        "-170.7 -362.7, -186.5 -366, -202.2 -367.7, -217.6 -368, -232.8 -366.8, -247.5 -364.2, -261.8 -360.3, " +
        "-275.5 -355.1, -288.5 -348.8, -300.8 -341.2, -312.3 -332.6, -322.9 -322.9, -332.6 -312.3, -341.2 -300.8, " +
        "-348.8 -288.5, -355.1 -275.5, -360.3 -261.8, -364.2 -247.5, -366.8 -232.8, -368 -217.6, -367.7 -202.2, " +
        "-366 -186.5, -362.7 -170.7, -357.9 -154.9, -351.3 -139.1, -343.1 -123.5, -333 -108.2, -321 -93.3, " +
        "-307 -78.8, -290.8 -65, -272.2 -51.9, -250.7 -39.7, -225.9 -28.5, -196.8 -18.6, -161.4 -10.2, " +
        "-114.4 -3.6, -45.6 -1.4, -45.7 0, -45.3 5.7, -44.2 11.4, -42.5 16.8, -40 22, -36.9 26.8, -33.3 31.3, " +
        "-29.1 35.2, -24.5 38.6, -19.4 41.3, -14.1 43.4, -8.6 44.9, -2.9 45.6, 2.9 45.6, 8.6 44.9, 14.1 43.4, " +
        "19.4 41.3, 24.5 38.6, 29.1 35.2, 33.3 31.3, 36.9 26.8, 40 22, 42.5 16.8, 44.2 11.4, 45.3 5.7, 45.7 0, " +
        "45.3 -5.7, 44.2 -11.4, 42.5 -16.8, 40 -22, 36.9 -26.8, 33.3 -31.3, 29.1 -35.2, 24.5 -38.6, 19.4 -41.3, " +
        "14.1 -43.4, 8.6 -44.9, 2.9 -45.6, -1.4 -45.6))",
      sridPlanar)
    val expectedConchoidWithBackLobe = GeomUtils.parseWkt(
      "POLYGON ((-23.6 -39.1, -25.8 -42, -43.9 -60.5, -65.3 -76.4, -89.4 -89.4, -115.8 -98.9, -144.1 -104.7, " +
        "-173.5 -106.3, -203.6 -103.7, -233.6 -96.7, -262.8 -85.4, -290.7 -69.8, -316.6 -50.1, -339.9 -26.7, " +
        "-359.9 0, -376.4 29.6, -388.8 61.6, -396.7 95.2, -400.1 130, -398.7 165.1, -392.5 200, -381.6 233.9, " +
        "-366.2 266, -346.5 295.9, -322.9 322.9, -295.9 346.5, -266 366.2, -233.9 381.6, -200 392.5, -165.1 398.7, " +
        "-130 400.1, -95.2 396.7, -61.6 388.8, -29.6 376.4, 0 359.9, 26.7 339.9, 50.1 316.6, 69.8 290.7, " +
        "85.4 262.8, 96.7 233.6, 103.7 203.6, 106.3 173.5, 104.7 144.1, 98.9 115.8, 89.4 89.4, 76.4 65.3, " +
        "60.5 43.9, 42 25.8, 39 23.6, 40 22, 42.5 16.8, 44.2 11.4, 45.3 5.7, 45.7 0, 45.3 -5.7, 44.2 -11.4, " +
        "42.5 -16.8, 40 -22, 36.9 -26.8, 33.3 -31.3, 29.1 -35.2, 24.5 -38.6, 19.4 -41.3, 14.1 -43.4, 8.6 -44.9, " +
        "2.9 -45.6, -2.9 -45.6, -8.6 -44.9, -14.1 -43.4, -19.4 -41.3, -23.6 -39.1))",
      sridPlanar)
  }

  "CellCoverage" should "calculate cell range for 2G antennas" in new WithCellCoverageShapes {
    CellCoverage.cellRange(tilt, height, TwoG) should be (rangeTwoG +- rangeTolerance)
  }

  it should "calculate cell range for 3G antennas" in new WithCellCoverageShapes {
    CellCoverage.cellRange(tilt, height, ThreeG) should be (rangeThreeGFourGFdd +- rangeTolerance)
  }

  it should "calculate cell range for 4G (FDD) antennas" in new WithCellCoverageShapes {
    CellCoverage.cellRange(tilt, height, FourGFdd) should be (rangeThreeGFourGFdd +- rangeTolerance)
  }

  it should "calculate cell range for 4G (TDD) antennas" in new WithCellCoverageShapes {
    CellCoverage.cellRange(tilt, height, FourGTdd) should be (rangeFourGTdd +- rangeTolerance)
  }

  it should "build the coverage geometry for omnidirectional antennas" in new WithCellCoverageShapes {
    CellCoverage.cellShape(position, height, 0, 360, tilt, tech) should equalGeometry (expectedCircle)
  }

  it should "build the coverage geometry for directional antennas (circular sectors)" in new WithCellCoverageShapes {
    CellCoverage.cellShape(
      position,
      height,
      azimuthCirSect,
      beamwidthCirSect,
      tilt,
      tech,
      CoverageModel.CircularSectors) should equalGeometry (expectedCirSect)
  }

  it should "build the coverage geometry for directional antennas (petals, beamwidth < 180)" in
    new WithCellCoverageShapes {
      CellCoverage.cellShape(position, height, azimuthHippopede, beamwidthLt180, tilt, tech) should
        equalGeometry (expectedHippopedeWithBackLobe)
    }

  it should "build the coverage geometry for directional antennas (petals, beamwidth >= 180)" in
    new WithCellCoverageShapes {
      CellCoverage.cellShape(position, height, azimuthConchoid, beamwidthGe180, tilt, tech) should
        equalGeometry (expectedConchoidWithBackLobe)
  }
}
