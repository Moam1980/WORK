/*
 * TODO: License goes here!
 */

package sa.com.mobily.flickering

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell._
import sa.com.mobily.geometry._

class FlickeringTest extends FlatSpec with ShouldMatchers {

  trait WithCells {
    val shapeWkt1 = "POLYGON (( 0 0, 0 4, 2 4, 2 0, 0 0 ))"
    val shapeWkt2 = "POLYGON (( 13 14, 17 14, 17 10, 13 14 ))"
    val shapeWkt3 = "POLYGON (( 3 4, 7 4, 7 0, 5 0, 3 4 ))"
    val shapeWkt4 = "POLYGON (( 3 4, 7 4, 7 0, 4 0, 4 2, 3 2, 3 4 ))"
    val shapeWkt5 = "POLYGON (( 0 5, 0 7, 2 7, 2 5, 0 5 ))"
    val shapeWkt6 = "POLYGON (( 1 0, 1 2, 3 2, 3 0, 1 0 ))"

    val cell1 = Cell(
      mcc = "420",
      mnc = "03",
      cellId = 4465390,
      lacTac = 51,
      planarCoords = UtmCoordinates(1, 4, "EPSG:32638"),
      technology = FourGTdd,
      cellType = Macro,
      height = 25,
      azimuth = 0,
      beamwidth = 90,
      range = 2530.3,
      coverageWkt = shapeWkt1)
    val cell2 = cell1.copy(lacTac = 52, coverageWkt = shapeWkt2, planarCoords = UtmCoordinates(13, 14, "EPSG:32638"))
    val cell3 = cell1.copy(lacTac = 53, coverageWkt = shapeWkt3, planarCoords = UtmCoordinates(3, 4, "EPSG:32638"))
    val cell4 = cell1.copy(lacTac = 54, coverageWkt = shapeWkt4, planarCoords = UtmCoordinates(3, 4, "EPSG:32638"))
    val cell5 = cell1.copy(lacTac = 55, coverageWkt = shapeWkt5, planarCoords = UtmCoordinates(0, 5, "EPSG:32638"))
    val cell6 = cell1.copy(lacTac = 56, coverageWkt = shapeWkt6, planarCoords = UtmCoordinates(1, 0, "EPSG:32638"))

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
    val cellIds1 = (cell1.lacTac, cell1.cellId)
    val cellIds2 = (cell2.lacTac, cell2.cellId)
    val cellIds3 = (cell3.lacTac, cell3.cellId)

    val flickeringCells1 = FlickeringCells(Set(cellIds1, cellIds2))
    val flickeringCells2 = FlickeringCells(Set(cellIds2, cellIds3))
    val flickeringCells3 = FlickeringCells(Set(cellIds1, cellIds3))
    val flickeringCells = Set(flickeringCells1, flickeringCells2, flickeringCells3)
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

  trait WithAnalyzeCells {

    val shapeWkt1: String = "POLYGON ((671950.3 2733145.3, 671934.5 2733147.9, 671892.7 2733162.1, " +
      "671856.3 2733180.1, 671823.6 2733201.7, 671794.5 2733226.7, 671768.9 2733254.7, 671747 2733285.4, " +
      "671729 2733318.3, 671715.1 2733353, 671705.5 2733389, 671700.2 2733425.8, 671699.3 2733462.9, " +
      "671702.8 2733499.9, 671710.7 2733536.1, 671722.7 2733571.1, 671738.8 2733604.4, 671758.7 2733635.6, " +
      "671782.1 2733664.2, 671808.7 2733689.9, 671838.2 2733712.2, 671870 2733730.9, 671903.9 2733745.7, " +
      "671939.2 2733756.4, 671975.5 2733762.9, 672012.4 2733765.1, 672049.3 2733762.9, 672085.6 2733756.4, " +
      "672120.9 2733745.7, 672154.8 2733730.9, 672186.6 2733712.2, 672216.1 2733689.9, 672242.7 2733664.2, " +
      "672266.1 2733635.6, 672286 2733604.4, 672302.1 2733571.1, 672314.1 2733536.1, 672322 2733499.9, " +
      "672325.5 2733462.9, 672324.6 2733425.8, 672319.3 2733389, 672309.7 2733353, 672295.8 2733318.3, " +
      "672277.8 2733285.4, 672255.9 2733254.7, 672230.3 2733226.7, 672201.2 2733201.7, 672168.5 2733180.1, " +
      "672132.1 2733162.1, 672090.3 2733147.9, 672074.5 2733145.3, 672074.9 2733143, 672075.4 2733135.1, " +
      "672074.9 2733127.2, 672073.4 2733119.4, 672071 2733111.9, 672067.6 2733104.7, 672063.4 2733098.1, " +
      "672058.3 2733092, 672052.6 2733086.6, 672046.2 2733081.9, 672039.2 2733078.1, 672031.9 2733075.2, " +
      "672024.2 2733073.2, 672016.4 2733072.2, 672008.4 2733072.2, 672000.6 2733073.2, 671992.9 2733075.2, " +
      "671985.6 2733078.1, 671978.6 2733081.9, 671972.2 2733086.6, 671966.5 2733092, 671961.4 2733098.1, " +
      "671957.2 2733104.7, 671953.8 2733111.9, 671951.4 2733119.4, 671949.9 2733127.2, 671949.4 2733135.1, " +
      "671949.9 2733143, 671950.3 2733145.3))"
    val shapeWkt2 = "POLYGON ((671950.3 2733145.3, 671934.5 2733147.9, 671892.7 2733162.1, 671856.3 2733180.1, " +
      "671823.6 2733201.7, 671794.5 2733226.7, 671768.9 2733254.7, 671747 2733285.4, 671729 2733318.3, " +
      "671715.1 2733353, 671705.5 2733389, 671700.2 2733425.8, 671699.3 2733462.9, 671702.8 2733499.9, " +
      "671710.7 2733536.1, 671722.7 2733571.1, 671738.8 2733604.4, 671758.7 2733635.6, 671782.1 2733664.2, " +
      "671808.7 2733689.9, 671838.2 2733712.2, 671870 2733730.9, 671903.9 2733745.7, 671939.2 2733756.4, " +
      "671975.5 2733762.9, 672012.4 2733765.1, 672049.3 2733762.9, 672085.6 2733756.4, 672120.9 2733745.7, " +
      "672154.8 2733730.9, 672186.6 2733712.2, 672216.1 2733689.9, 672242.7 2733664.2, 672266.1 2733635.6, " +
      "672286 2733604.4, 672302.1 2733571.1, 672314.1 2733536.1, 672322 2733499.9, 672325.5 2733462.9, " +
      "672324.6 2733425.8, 672319.3 2733389, 672309.7 2733353, 672295.8 2733318.3, 672277.8 2733285.4, " +
      "672255.9 2733254.7, 672230.3 2733226.7, 672201.2 2733201.7, 672168.5 2733180.1, 672132.1 2733162.1, " +
      "672090.3 2733147.9, 672074.5 2733145.3, 672074.9 2733143, 672075.4 2733135.1, 672074.9 2733127.2, " +
      "672073.4 2733119.4, 672071 2733111.9, 672067.6 2733104.7, 672063.4 2733098.1, 672058.3 2733092, " +
      "672052.6 2733086.6, 672046.2 2733081.9, 672039.2 2733078.1, 672031.9 2733075.2, 672024.2 2733073.2, " +
      "672016.4 2733072.2, 672008.4 2733072.2, 672000.6 2733073.2, 671992.9 2733075.2, 671985.6 2733078.1, " +
      "671978.6 2733081.9, 671972.2 2733086.6, 671966.5 2733092, 671961.4 2733098.1, 671957.2 2733104.7, " +
      "671953.8 2733111.9, 671951.4 2733119.4, 671949.9 2733127.2, 671949.4 2733135.1, 671949.9 2733143, " +
      "671950.3 2733145.3))"
    val shapeWkt3 = "POLYGON ((669525.7 2733312.6, 669542 2733322.3, 669590.6 2733341.3, 669637.2 2733352.7, " +
      "669683.2 2733357.9, 669728.6 2733357.4, 669773 2733351.5, 669816.2 2733340.4, 669857.4 2733324.2, " +
      "669896.4 2733303.4, 669932.4 2733278.1, 669965.2 2733248.8, 669994.2 2733215.8, 670019.1 2733179.7, " +
      "670039.5 2733141, 670055.2 2733100.1, 670065.9 2733057.7, 670071.6 2733014.4, 670072.1 2732970.7, " +
      "670067.5 2732927.3, 670057.8 2732884.7, 670043.2 2732843.6, 670023.8 2732804.5, 670000 2732767.9, " +
      "669972 2732734.4, 669940.3 2732704.4, 669905.3 2732678.4, 669867.4 2732656.7, 669827.3 2732639.6, " +
      "669785.4 2732627.3, 669742.3 2732620, 669698.7 2732617.8, 669655.2 2732620.8, 669612.2 2732629, " +
      "669570.5 2732642.1, 669530.7 2732660, 669493.2 2732682.6, 669458.5 2732709.4, 669427.3 2732740.2, " +
      "669399.9 2732774.4, 669376.7 2732811.8, 669358.1 2732851.7, 669344.3 2732893.7, 669335.6 2732937.1, " +
      "669332.1 2732981.5, 669334 2733026.3, 669341.4 2733071.1, 669354.5 2733115.5, 669373.8 2733159.4, " +
      "669400.9 2733204, 669413.2 2733218.3, 669407.4 2733223.8, 669401.5 2733231, 669396.5 2733238.9, " +
      "669392.5 2733247.4, 669389.6 2733256.3, 669387.8 2733265.5, 669387.2 2733274.8, 669387.8 2733284.1, " +
      "669389.6 2733293.3, 669392.5 2733302.2, 669396.5 2733310.7, 669401.5 2733318.6, 669407.4 2733325.8, " +
      "669414.2 2733332.2, 669421.8 2733337.7, 669430 2733342.2, 669438.7 2733345.6, 669447.7 2733347.9, " +
      "669457 2733349.1, 669466.4 2733349.1, 669475.7 2733347.9, 669484.7 2733345.6, 669493.4 2733342.2, " +
      "669501.6 2733337.7, 669509.2 2733332.2, 669516 2733325.8, 669521.9 2733318.6, 669525.7 2733312.6))"
    val shapeWkt4 = "POLYGON ((669357.5 2733346.2, 669357.3 2733346.2, 669291.5 2733364.2, 669231.1 2733389.3, " +
      "669175.1 2733421.4, 669123.8 2733459.8, 669077.9 2733504, 669037.7 2733553.4, 669004 2733607.3, " +
      "668977.1 2733664.8, 668957.3 2733725.1, 668945.1 2733787.3, 668940.6 2733850.5, 668943.7 2733913.8, " +
      "668954.5 2733976.2, 668972.8 2734036.8, 668998.4 2734094.7, 669030.7 2734149.1, 669069.4 2734199.2, " +
      "669114 2734244.2, 669163.6 2734283.4, 669217.6 2734316.4, 669275.3 2734342.5, 669335.6 2734361.4, " +
      "669397.8 2734372.9, 669461 2734376.8, 669524.2 2734372.9, 669586.4 2734361.4, 669646.7 2734342.5, " +
      "669704.4 2734316.4, 669758.4 2734283.4, 669808 2734244.2, 669852.6 2734199.2, 669891.3 2734149.1, " +
      "669923.6 2734094.7, 669949.2 2734036.8, 669967.5 2733976.2, 669978.3 2733913.8, 669981.4 2733850.5, " +
      "669976.9 2733787.3, 669964.7 2733725.1, 669944.9 2733664.8, 669918 2733607.3, 669884.3 2733553.4, " +
      "669844.1 2733504, 669798.2 2733459.8, 669746.9 2733421.4, 669690.9 2733389.3, 669630.5 2733364.2, " +
      "669564.7 2733346.2, 669564.5 2733346.2, 669565.2 2733334.4, 669564.4 2733321.3, 669562 2733308.5, " +
      "669557.9 2733296, 669552.3 2733284.2, 669545.3 2733273.1, 669537 2733263, 669527.4 2733254.1, " +
      "669516.9 2733246.4, 669505.4 2733240.1, 669493.2 2733235.3, 669480.5 2733232, 669467.5 2733230.4, " +
      "669454.5 2733230.4, 669441.5 2733232, 669428.8 2733235.3, 669416.6 2733240.1, 669405.1 2733246.4, " +
      "669394.6 2733254.1, 669385 2733263, 669376.7 2733273.1, 669369.7 2733284.2, 669364.1 2733296, " +
      "669360 2733308.5, 669357.6 2733321.3, 669356.8 2733334.4, 669357.5 2733346.2))"

    val cell1 = Cell(
      cellId = 12121,
      lacTac = 1324,
      planarCoords = UtmCoordinates(672012.4, 2733135.1, "EPSG:32638"),
      technology = ThreeG,
      cellType = Macro,
      height = 22.0,
      azimuth = 0.0,
      beamwidth =  168.0,
      range = 629.99757222,
      coverageWkt = shapeWkt1,
      mcc = "420",
      mnc = "03")
    val cell2 = Cell(
      cellId = 12369,
      lacTac = 1329,
      planarCoords = UtmCoordinates(670886.1, 2732604.2, "EPSG:32638"),
      technology = ThreeG,
      cellType = Macro,
      height = 19.0,
      azimuth = 320.0,
      beamwidth =  168.0,
      range = 544.08881238,
      coverageWkt = shapeWkt1,
      mcc = "420",
      mnc = "03")
    val cell3 = Cell(
      cellId = 13062,
      lacTac = 1324,
      planarCoords = UtmCoordinates(669461.7, 2733274.8, "EPSG:32638"),
      technology = ThreeG,
      cellType = Macro,
      height = 26.0,
      azimuth = 140.0,
      beamwidth =  168.0,
      range = 744.54258536,
      coverageWkt = shapeWkt3,
      mcc = "420",
      mnc = "03")
    val cell4 = Cell(
      cellId = 13061,
      lacTac = 1012,
      planarCoords = UtmCoordinates(669461.0, 2733334.4, "EPSG:32638"),
      technology = TwoG,
      cellType = Macro,
      height = 25.5,
      azimuth = 0.0,
      beamwidth =  174.0,
      range = 1042.3596195,
      coverageWkt = shapeWkt4,
      mcc = "420",
      mnc = "03")
    val cellCatalogue = Map(
      ((cell1.lacTac, cell1.cellId), cell1),
      ((cell2.lacTac, cell2.cellId), cell2),
      ((cell3.lacTac, cell3.cellId), cell3),
      ((cell4.lacTac, cell4.cellId), cell4))
  }

  trait WithAnalyzeFlickeringCells extends WithAnalyzeCells {

    val growthFactor = 2f
    val rangeTolerance = 1
    val intersectRatio = 0.04955471180328642
    val flickeringCells1 = FlickeringCells(Set((cell1.lacTac, cell1.cellId), (cell2.lacTac, cell2.cellId)))
    val flickeringCells2 = FlickeringCells(Set((cell3.lacTac, cell3.cellId), (cell4.lacTac, cell4.cellId)))
  }

  "FlickeringDetector" should "detect flickering" in new WithFlickeringCells {
    val flickering = Flickering.detect(timeCells, 5)(cellCatalogue)
    flickering.size should be (3)
    flickering should be (flickeringCells)
  }

  it should "not detect flickering in range with one cell" in new WithOneRepeatedCell {
    val flickering = Flickering.detect(timeCells, 5)(cellCatalogue)
    flickering.size should be (0)
  }

  it should "not detect flickering in range with non flickering cells" in new WithNonFlickeringCells {
    val flickering = Flickering.detect(timeCells, 5)(cellCatalogue)
    flickering.size should be (0)
  }

  it should "analyze the non solvable flickering between cells" in new WithAnalyzeFlickeringCells {
    val nonSolvableFlickering = Flickering.analysis(flickeringCells1, growthFactor)(cellCatalogue)
    nonSolvableFlickering.isRight should be (true)
    nonSolvableFlickering.right.get should be (flickeringCells1)
  }

  it should "analyze the solvable flickering between cells" in new WithAnalyzeFlickeringCells {
    val solvableFlickering = Flickering.analysis(flickeringCells2, growthFactor)(cellCatalogue)
    solvableFlickering.isLeft should be (true)
    solvableFlickering.left.get._1 should be (intersectRatio +- rangeTolerance)
    solvableFlickering.left.get._2 should be (flickeringCells2)
  }
}
