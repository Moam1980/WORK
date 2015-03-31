/*
 * TODO: License goes here!
 */

package sa.com.mobily.location.spark

import com.github.nscala_time.time.Imports._
import org.scalatest._

import sa.com.mobily.cell.{Cell, FourGFdd, Micro}
import sa.com.mobily.geometry.{Coordinates, GeomUtils, UtmCoordinates}
import sa.com.mobily.location._
import sa.com.mobily.mobility.MobilityMatrixItem
import sa.com.mobily.poi._
import sa.com.mobily.roaming.CountryCode
import sa.com.mobily.user.User
import sa.com.mobily.usercentric.Dwell
import sa.com.mobily.usercentric.spark.UserModelDsl
import sa.com.mobily.utils.{EdmCoreUtils, EdmCustomMatchers, LocalSparkContext}

class LocationDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext with EdmCustomMatchers {

  import LocationDsl._
  import UserModelDsl._

  trait WithLocationText {

    val locationText1 = "locationTest|clientTest|EPSG:32638|POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))"
    val locationText2 = "anotherLocation|clientTest|EPSG:32638|POLYGON ((10 10, 10 12, 12 12, 12 10, 10 10))"
    val locationText3 = "thirdLocation|clientTest|EPSG:32638|POLYGON ((100 100, 100 102, 102 102, 102 100, 100 100))"
    val locationText4 = "fourthLocation|clientTest|EPSG:32638|POLYGON ((100 100, 100 102, 102 102, 102 100, 100 100))|"

    val locationsText = sc.parallelize(List(locationText1, locationText2, locationText3, locationText4))
  }

  trait WithLocations {

    val location1 = Location(name = "locationTest", client = "clientTest", epsg = "EPSG:32638",
      "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))")
    val location2 = Location(name = "anotherLocation", client = "clientTest", epsg = "EPSG:32638",
      "POLYGON ((10 10, 10 12, 12 12, 12 10, 10 10))")
    val location3 = Location(name = "thirdLocation", client = "clientTest", epsg = "EPSG:32638",
      "POLYGON ((100 100, 100 102, 102 102, 102 100, 100 100))")

    val locations = sc.parallelize(List(location1, location2, location3))
  }

  trait WithLocationsCentroids extends WithLocations {

    val locationCentroid1 = location1.copy(geomWkt = "POINT (1 1)")
    val locationCentroid2 = location2.copy(geomWkt = "POINT (11 11)")
    val locationCentroid3 = location3.copy(geomWkt = "POINT (101 101)")

    val locationsCentroids = Array(locationCentroid1, locationCentroid2, locationCentroid3)
  }

  trait WithLocationsPointsWgs84 {

    val shapeWkt = "POLYGON ((1006463.8 2937269.6, 1006540.2 2937231.2, 1006600.1 2937188.9, 1006643.9 2937148.7, " +
      "1006678 2937108.6, 1006704.9 2937068.1, 1006725.8 2937027.2, 1006741.2 2936985.9, 1006751.5 2936944.6, " +
      "1006757.2 2936903.5, 1006758.3 2936862.9, 1006755.2 2936823.1, 1006747.9 2936784.4, 1006736.8 2936747.2, " +
      "1006721.9 2936711.6, 1006703.6 2936678.1, 1006682.1 2936646.9, 1006657.7 2936618.2, 1006630.6 2936592.3, " +
      "1006601.1 2936569.4, 1006569.5 2936549.7, 1006536.2 2936533.3, 1006501.4 2936520.4, 1006465.6 2936511.1, " +
      "1006429.1 2936505.6, 1006392.2 2936503.7, 1006355.3 2936505.6, 1006318.8 2936511.1, 1006283 2936520.4, " +
      "1006248.2 2936533.3, 1006214.9 2936549.7, 1006183.3 2936569.4, 1006153.8 2936592.3, 1006126.7 2936618.2, " +
      "1006102.3 2936646.9, 1006080.8 2936678.1, 1006062.5 2936711.6, 1006047.6 2936747.2, 1006036.5 2936784.4, " +
      "1006029.2 2936823.1, 1006026.1 2936862.9, 1006027.2 2936903.5, 1006032.9 2936944.6, 1006043.2 2936985.9, " +
      "1006058.6 2937027.2, 1006079.5 2937068.1, 1006106.4 2937108.6, 1006140.5 2937148.7, 1006184.3 2937188.9, " +
      "1006244.2 2937231.2, 1006320.6 2937269.6, 1006317.6 2937276, 1006314.5 2937285.6, 1006312.7 2937295.5, " +
      "1006312 2937305.5, 1006312.7 2937315.5, 1006314.5 2937325.4, 1006317.6 2937335, 1006321.9 2937344.1, " +
      "1006327.3 2937352.6, 1006333.8 2937360.4, 1006341.1 2937367.3, 1006349.2 2937373.2, 1006358.1 2937378.1, " +
      "1006367.4 2937381.8, 1006377.2 2937384.3, 1006387.2 2937385.5, 1006397.2 2937385.5, 1006407.2 2937384.3, " +
      "1006417 2937381.8, 1006426.3 2937378.1, 1006435.2 2937373.2, 1006443.3 2937367.3, 1006450.6 2937360.4, " +
      "1006457.1 2937352.6, 1006462.5 2937344.1, 1006466.8 2937335, 1006469.9 2937325.4, 1006471.7 2937315.5, " +
      "1006472.4 2937305.5, 1006471.7 2937295.5, 1006469.9 2937285.6, 1006466.8 2937276, 1006463.8 2937269.6))"
    val geom = GeomUtils.parseWkt(shapeWkt, Coordinates.SaudiArabiaUtmSrid, Coordinates.UtmPrecisionModel)

    val shapeWgs84Wkt = "POLYGON ((26.46564199 50.07687119, 26.46526943 50.07761943, 26.46486766 50.07820124, " +
      "26.46449051 50.078623, 26.46411772 50.07894787, 26.46374389 50.07920064, 26.46336861 50.07939329, " +
      "26.46299168 50.07953083, 26.46261657 50.0796174, 26.4622449 50.07965809, 26.46187937 50.07965301, " +
      "26.46152252 50.07960628, 26.46117707 50.07951802, 26.46084646 50.07939239, 26.46053159 50.07922941, " +
      "26.46023682 50.0790333, 26.45996388 50.07880613, 26.45971445 50.07855096, 26.45949116 50.07826993, " +
      "26.45929571 50.07796611, 26.45912979 50.07764256, 26.45899415 50.07730334, 26.45889052 50.07695052, " +
      "26.45881962 50.07658913, 26.45878315 50.07622224, 26.4587792 50.07585279, 26.45880942 50.07548483, " +
      "26.45887188 50.0751223, 26.45896826 50.07476826, 26.45909666 50.07442564, 26.45925601 50.07409938, " +
      "26.45944443 50.07379142, 26.45966088 50.07350571, 26.45990346 50.07324516, 26.46017026 50.07301269, " +
      "26.46045852 50.07281019, 26.46076632 50.07264057, 26.4610918 50.07250576, 26.46143031 50.07240955, " +
      "26.46178097 50.0723519, 26.46214003 50.07233665, 26.46250478 50.07236369, 26.4628724 50.07243689, " +
      "26.46324017 50.07255614, 26.46360614 50.07272635, 26.46396655 50.07295136, 26.46432122 50.07323617, " +
      "26.46466974 50.07359277, 26.4650157 50.07404634, 26.46537481 50.07466162, 26.46569298 50.07544025, " +
      "26.46575161 50.0754128, 26.46583905 50.07538562, 26.46592873 50.07537155, 26.46601891 50.07536851, " +
      "26.4661086 50.07537946, 26.466197 50.07540136, 26.46628223 50.07543614, 26.46636255 50.0754827, " +
      "26.46643707 50.07554003, 26.46650491 50.07560806, 26.46656437 50.07568374, 26.46661454 50.07576702, " +
      "26.46665544 50.07585789, 26.46668541 50.07595228, 26.46670441 50.0760512, 26.46671164 50.0761516, " +
      "26.46670808 50.07625153, 26.46669372 50.07635098, 26.46666775 50.07644792, 26.46663116 50.07653939, " +
      "26.46658392 50.07662639, 26.46652798 50.07670499, 26.46646332 50.07677521, 26.46639085 50.07683708, " +
      "26.46631248 50.07688767, 26.46622911 50.07692704, 26.46614167 50.07695422, 26.46605199 50.07696829, " +
      "26.4659618 50.07697133, 26.46587211 50.07696038, 26.46578372 50.07693847, 26.46569848 50.0769037, " +
      "26.46564199 50.07687119))"
    val geomWgs84 = GeomUtils.parseWkt(shapeWgs84Wkt, Coordinates.Wgs84GeodeticSrid, Coordinates.LatLongPrecisionModel)

    val location =
      Location(
        name = "locationTest",
        client = "clientTest",
        epsg = Coordinates.SaudiArabiaUtmEpsg,
        geomWkt = shapeWkt)

    val locationWgs84 = location.copy(epsg = Coordinates.Wgs84GeodeticEpsg, geomWkt = shapeWgs84Wkt)

    val locationsPointsView = Array(
      LocationPointView("clientTest", "locationTest", 1, 1, 26.46564199D, 50.07687119D),
      LocationPointView("clientTest", "locationTest", 1, 2, 26.46526943D, 50.07761943D),
      LocationPointView("clientTest", "locationTest", 1, 3, 26.46486766D, 50.07820124D),
      LocationPointView("clientTest", "locationTest", 1, 4, 26.46449051D, 50.078623D),
      LocationPointView("clientTest", "locationTest", 1, 5, 26.46411772D, 50.07894787D),
      LocationPointView("clientTest", "locationTest", 1, 6, 26.46374389D, 50.07920064D),
      LocationPointView("clientTest", "locationTest", 1, 7, 26.46336861D, 50.07939329D),
      LocationPointView("clientTest", "locationTest", 1, 8, 26.46299168D, 50.07953083D),
      LocationPointView("clientTest", "locationTest", 1, 9, 26.46261657D, 50.0796174D),
      LocationPointView("clientTest", "locationTest", 1, 10, 26.4622449D, 50.07965809D),
      LocationPointView("clientTest", "locationTest", 1, 11, 26.46187937D, 50.07965301D),
      LocationPointView("clientTest", "locationTest", 1, 12, 26.46152252D, 50.07960628D),
      LocationPointView("clientTest", "locationTest", 1, 13, 26.46117707D, 50.07951802D),
      LocationPointView("clientTest", "locationTest", 1, 14, 26.46084646D, 50.07939239D),
      LocationPointView("clientTest", "locationTest", 1, 15, 26.46053159D, 50.07922941D),
      LocationPointView("clientTest", "locationTest", 1, 16, 26.46023682D, 50.0790333D),
      LocationPointView("clientTest", "locationTest", 1, 17, 26.45996388D, 50.07880613D),
      LocationPointView("clientTest", "locationTest", 1, 18, 26.45971445D, 50.07855096D),
      LocationPointView("clientTest", "locationTest", 1, 19, 26.45949116D, 50.07826993D),
      LocationPointView("clientTest", "locationTest", 1, 20, 26.45929571D, 50.07796611D),
      LocationPointView("clientTest", "locationTest", 1, 21, 26.45912979D, 50.07764256D),
      LocationPointView("clientTest", "locationTest", 1, 22, 26.45899415D, 50.07730334D),
      LocationPointView("clientTest", "locationTest", 1, 23, 26.45889052D, 50.07695052D),
      LocationPointView("clientTest", "locationTest", 1, 24, 26.45881962D, 50.07658913D),
      LocationPointView("clientTest", "locationTest", 1, 25, 26.45878315D, 50.07622224D),
      LocationPointView("clientTest", "locationTest", 1, 26, 26.4587792D, 50.07585279D),
      LocationPointView("clientTest", "locationTest", 1, 27, 26.45880942D, 50.07548483D),
      LocationPointView("clientTest", "locationTest", 1, 28, 26.45887188D, 50.0751223D),
      LocationPointView("clientTest", "locationTest", 1, 29, 26.45896826D, 50.07476826D),
      LocationPointView("clientTest", "locationTest", 1, 30, 26.45909666D, 50.07442564D),
      LocationPointView("clientTest", "locationTest", 1, 31, 26.45925601D, 50.07409938D),
      LocationPointView("clientTest", "locationTest", 1, 32, 26.45944443D, 50.07379142D),
      LocationPointView("clientTest", "locationTest", 1, 33, 26.45966088D, 50.07350571D),
      LocationPointView("clientTest", "locationTest", 1, 34, 26.45990346D, 50.07324516D),
      LocationPointView("clientTest", "locationTest", 1, 35, 26.46017026D, 50.07301269D),
      LocationPointView("clientTest", "locationTest", 1, 36, 26.46045852D, 50.07281019D),
      LocationPointView("clientTest", "locationTest", 1, 37, 26.46076632D, 50.07264057D),
      LocationPointView("clientTest", "locationTest", 1, 38, 26.4610918D, 50.07250576D),
      LocationPointView("clientTest", "locationTest", 1, 39, 26.46143031D, 50.07240955D),
      LocationPointView("clientTest", "locationTest", 1, 40, 26.46178097D, 50.0723519D),
      LocationPointView("clientTest", "locationTest", 1, 41, 26.46214003D, 50.07233665D),
      LocationPointView("clientTest", "locationTest", 1, 42, 26.46250478D, 50.07236369D),
      LocationPointView("clientTest", "locationTest", 1, 43, 26.4628724D, 50.07243689D),
      LocationPointView("clientTest", "locationTest", 1, 44, 26.46324017D, 50.07255614D),
      LocationPointView("clientTest", "locationTest", 1, 45, 26.46360614D, 50.07272635D),
      LocationPointView("clientTest", "locationTest", 1, 46, 26.46396655D, 50.07295136D),
      LocationPointView("clientTest", "locationTest", 1, 47, 26.46432122D, 50.07323617D),
      LocationPointView("clientTest", "locationTest", 1, 48, 26.46466974D, 50.07359277D),
      LocationPointView("clientTest", "locationTest", 1, 49, 26.4650157D, 50.07404634D),
      LocationPointView("clientTest", "locationTest", 1, 50, 26.46537481D, 50.07466162D),
      LocationPointView("clientTest", "locationTest", 1, 51, 26.46569298D, 50.07544025D),
      LocationPointView("clientTest", "locationTest", 1, 52, 26.46575161D, 50.0754128D),
      LocationPointView("clientTest", "locationTest", 1, 53, 26.46583905D, 50.07538562D),
      LocationPointView("clientTest", "locationTest", 1, 54, 26.46592873D, 50.07537155D),
      LocationPointView("clientTest", "locationTest", 1, 55, 26.46601891D, 50.07536851D),
      LocationPointView("clientTest", "locationTest", 1, 56, 26.4661086D, 50.07537946D),
      LocationPointView("clientTest", "locationTest", 1, 57, 26.466197D, 50.07540136D),
      LocationPointView("clientTest", "locationTest", 1, 58, 26.46628223D, 50.07543614D),
      LocationPointView("clientTest", "locationTest", 1, 59, 26.46636255D, 50.0754827D),
      LocationPointView("clientTest", "locationTest", 1, 60, 26.46643707D, 50.07554003D),
      LocationPointView("clientTest", "locationTest", 1, 61, 26.46650491D, 50.07560806D),
      LocationPointView("clientTest", "locationTest", 1, 62, 26.46656437D, 50.07568374D),
      LocationPointView("clientTest", "locationTest", 1, 63, 26.46661454D, 50.07576702D),
      LocationPointView("clientTest", "locationTest", 1, 64, 26.46665544D, 50.07585789D),
      LocationPointView("clientTest", "locationTest", 1, 65, 26.46668541D, 50.07595228D),
      LocationPointView("clientTest", "locationTest", 1, 66, 26.46670441D, 50.0760512D),
      LocationPointView("clientTest", "locationTest", 1, 67, 26.46671164D, 50.0761516D),
      LocationPointView("clientTest", "locationTest", 1, 68, 26.46670808D, 50.07625153D),
      LocationPointView("clientTest", "locationTest", 1, 69, 26.46669372D, 50.07635098D),
      LocationPointView("clientTest", "locationTest", 1, 70, 26.46666775D, 50.07644792D),
      LocationPointView("clientTest", "locationTest", 1, 71, 26.46663116D, 50.07653939D),
      LocationPointView("clientTest", "locationTest", 1, 72, 26.46658392D, 50.07662639D),
      LocationPointView("clientTest", "locationTest", 1, 73, 26.46652798D, 50.07670499D),
      LocationPointView("clientTest", "locationTest", 1, 74, 26.46646332D, 50.07677521D),
      LocationPointView("clientTest", "locationTest", 1, 75, 26.46639085D, 50.07683708D),
      LocationPointView("clientTest", "locationTest", 1, 76, 26.46631248D, 50.07688767D),
      LocationPointView("clientTest", "locationTest", 1, 77, 26.46622911D, 50.07692704D),
      LocationPointView("clientTest", "locationTest", 1, 78, 26.46614167D, 50.07695422D),
      LocationPointView("clientTest", "locationTest", 1, 79, 26.46605199D, 50.07696829D),
      LocationPointView("clientTest", "locationTest", 1, 80, 26.4659618D, 50.07697133D),
      LocationPointView("clientTest", "locationTest", 1, 81, 26.46587211D, 50.07696038D),
      LocationPointView("clientTest", "locationTest", 1, 82, 26.46578372D, 50.07693847D),
      LocationPointView("clientTest", "locationTest", 1, 83, 26.46569848D, 50.0769037D),
      LocationPointView("clientTest", "locationTest", 1, 84, 26.46564199D, 50.07687119D))

    val locations = sc.parallelize(Array(location))
  }

  trait WithCellCatalogue {

    val cell1 = Cell(1, 1, UtmCoordinates(1, 4), FourGFdd, Micro, 20, 180, 45, 4, "1",
      "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))")
    val cell2 = cell1.copy(cellId = 2, coverageWkt = "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))")
    val cell3 = cell1.copy(cellId = 3, coverageWkt = "POLYGON ((0.5 0, 0.5 1, 1.5 1, 1.5 0, 0.5 0))")
    val cell4 = cell1.copy(cellId = 4, coverageWkt = "POLYGON ((10 10, 10 11, 11 11, 11 10, 10 10))")

    implicit val cellCatalogue = sc.parallelize(Array(cell1, cell2, cell3, cell4)).toBroadcastMap
  }

  trait WithEmptyCellCatalogue {

    implicit val cellCatalogue = sc.parallelize(Array[Cell]()).toBroadcastMap
  }

  trait WithLocationInWgs84 {

    val shapeWgs84Wkt = "POLYGON ((26.46564199 50.07687119, 26.46526943 50.07761943, 26.46486766 50.07820124, " +
      "26.46449051 50.078623, 26.46411772 50.07894787, 26.46374389 50.07920064, 26.46336861 50.07939329, " +
      "26.46299168 50.07953083, 26.46261657 50.0796174, 26.4622449 50.07965809, 26.46187937 50.07965301, " +
      "26.46152252 50.07960628, 26.46117707 50.07951802, 26.46084646 50.07939239, 26.46053159 50.07922941, " +
      "26.46023682 50.0790333, 26.45996388 50.07880613, 26.45971445 50.07855096, 26.45949116 50.07826993, " +
      "26.45929571 50.07796611, 26.45912979 50.07764256, 26.45899415 50.07730334, 26.45889052 50.07695052, " +
      "26.45881962 50.07658913, 26.45878315 50.07622224, 26.4587792 50.07585279, 26.45880942 50.07548483, " +
      "26.45887188 50.0751223, 26.45896826 50.07476826, 26.45909666 50.07442564, 26.45925601 50.07409938, " +
      "26.45944443 50.07379142, 26.45966088 50.07350571, 26.45990346 50.07324516, 26.46017026 50.07301269, " +
      "26.46045852 50.07281019, 26.46076632 50.07264057, 26.4610918 50.07250576, 26.46143031 50.07240955, " +
      "26.46178097 50.0723519, 26.46214003 50.07233665, 26.46250478 50.07236369, 26.4628724 50.07243689, " +
      "26.46324017 50.07255614, 26.46360614 50.07272635, 26.46396655 50.07295136, 26.46432122 50.07323617, " +
      "26.46466974 50.07359277, 26.4650157 50.07404634, 26.46537481 50.07466162, 26.46569298 50.07544025, " +
      "26.46575161 50.0754128, 26.46583905 50.07538562, 26.46592873 50.07537155, 26.46601891 50.07536851, " +
      "26.4661086 50.07537946, 26.466197 50.07540136, 26.46628223 50.07543614, 26.46636255 50.0754827, " +
      "26.46643707 50.07554003, 26.46650491 50.07560806, 26.46656437 50.07568374, 26.46661454 50.07576702, " +
      "26.46665544 50.07585789, 26.46668541 50.07595228, 26.46670441 50.0760512, 26.46671164 50.0761516, " +
      "26.46670808 50.07625153, 26.46669372 50.07635098, 26.46666775 50.07644792, 26.46663116 50.07653939, " +
      "26.46658392 50.07662639, 26.46652798 50.07670499, 26.46646332 50.07677521, 26.46639085 50.07683708, " +
      "26.46631248 50.07688767, 26.46622911 50.07692704, 26.46614167 50.07695422, 26.46605199 50.07696829, " +
      "26.4659618 50.07697133, 26.46587211 50.07696038, 26.46578372 50.07693847, 26.46569848 50.0769037, " +
      "26.46564199 50.07687119))"
    val geomWgs84 = GeomUtils.parseWkt(shapeWgs84Wkt, Coordinates.Wgs84GeodeticSrid, Coordinates.LatLongPrecisionModel)

    val location = Location(name = "locationTest", client = "clientTest", epsg = Coordinates.Wgs84GeodeticEpsg,
      geomWkt = shapeWgs84Wkt)

    val shapeWkt = "POLYGON ((1006463.8 2937269.6, 1006540.2 2937231.2, 1006600.1 2937188.9, 1006643.9 2937148.7, " +
      "1006678 2937108.6, 1006704.9 2937068.1, 1006725.8 2937027.2, 1006741.2 2936985.9, 1006751.5 2936944.6, " +
      "1006757.2 2936903.5, 1006758.3 2936862.9, 1006755.2 2936823.1, 1006747.9 2936784.4, 1006736.8 2936747.2, " +
      "1006721.9 2936711.6, 1006703.6 2936678.1, 1006682.1 2936646.9, 1006657.7 2936618.2, 1006630.6 2936592.3, " +
      "1006601.1 2936569.4, 1006569.5 2936549.7, 1006536.2 2936533.3, 1006501.4 2936520.4, 1006465.6 2936511.1, " +
      "1006429.1 2936505.6, 1006392.2 2936503.7, 1006355.3 2936505.6, 1006318.8 2936511.1, 1006283 2936520.4, " +
      "1006248.2 2936533.3, 1006214.9 2936549.7, 1006183.3 2936569.4, 1006153.8 2936592.3, 1006126.7 2936618.2, " +
      "1006102.3 2936646.9, 1006080.8 2936678.1, 1006062.5 2936711.6, 1006047.6 2936747.2, 1006036.5 2936784.4, " +
      "1006029.2 2936823.1, 1006026.1 2936862.9, 1006027.2 2936903.5, 1006032.9 2936944.6, 1006043.2 2936985.9, " +
      "1006058.6 2937027.2, 1006079.5 2937068.1, 1006106.4 2937108.6, 1006140.5 2937148.7, 1006184.3 2937188.9, " +
      "1006244.2 2937231.2, 1006320.6 2937269.6, 1006317.6 2937276, 1006314.5 2937285.6, 1006312.7 2937295.5, " +
      "1006312 2937305.5, 1006312.7 2937315.5, 1006314.5 2937325.4, 1006317.6 2937335, 1006321.9 2937344.1, " +
      "1006327.3 2937352.6, 1006333.8 2937360.4, 1006341.1 2937367.3, 1006349.2 2937373.2, 1006358.1 2937378.1, " +
      "1006367.4 2937381.8, 1006377.2 2937384.3, 1006387.2 2937385.5, 1006397.2 2937385.5, 1006407.2 2937384.3, " +
      "1006417 2937381.8, 1006426.3 2937378.1, 1006435.2 2937373.2, 1006443.3 2937367.3, 1006450.6 2937360.4, " +
      "1006457.1 2937352.6, 1006462.5 2937344.1, 1006466.8 2937335, 1006469.9 2937325.4, 1006471.7 2937315.5, " +
      "1006472.4 2937305.5, 1006471.7 2937295.5, 1006469.9 2937285.6, 1006466.8 2937276, 1006463.8 2937269.6))"
    val geom = GeomUtils.parseWkt(shapeWkt, Coordinates.SaudiArabiaUtmSrid, Coordinates.UtmPrecisionModel)

    val locations = sc.parallelize(Array(location))
  }

  trait WithDwellsForMatching {

    val location1 = Location(name = "location1", client = "client", epsg = Coordinates.SaudiArabiaUtmEpsg,
      geomWkt = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")
    val location2 = location1.copy(name = "location2", geomWkt = "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))")
    val locations = sc.parallelize(Array(location1, location2))

    val user1 = User(imei = "", imsi = "420031", msisdn = 9661)
    val user2 = User(imei = "", imsi = "420032", msisdn = 9662)
    val user3 = User(imei = "", imsi = "420033", msisdn = 9663)

    val dwell1 = Dwell(
      user = user1,
      startTime = 1000,
      endTime = 60000,
      geomWkt = "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
      cells = Seq((2, 4), (2, 6)),
      firstEventBeginTime = 3,
      lastEventEndTime = 9,
      numEvents = 2)
    val dwell2 = dwell1.copy(user = user2, startTime = 55000, endTime = 120000,
      geomWkt = "POLYGON ((12 12, 12 20, 20 20, 20 12, 12 12))")
    val dwell3 = dwell1.copy(user = user3, startTime = 40000, endTime = 53000,
      geomWkt = "POLYGON ((7.5 7.5, 7.5 11.5, 11.5 11.5, 11.5 7.5, 7.5 7.5))")
    val dwell4 = dwell3.copy(startTime = 55000, endTime = 58000)
    val dwell5 = dwell3.copy(startTime = 133000, endTime = 145000,
      geomWkt = "POLYGON ((30 30, 30 40, 40 40, 40 30, 30 30))")
    val dwells = sc.parallelize(Array(dwell1, dwell2, dwell3, dwell4, dwell5))
    val intervals = EdmCoreUtils.intervals(new DateTime(0), new DateTime(180000), 1)

    val footfallLoc1Time1 = Footfall(users = Set(user1, user3), numDwells = 3, avgPrecision = 44)
    val footfallLoc2Time1 = Footfall(users = Set(user2), numDwells = 1, avgPrecision = 64)
    val footfallLoc2Time2 = Footfall(users = Set(user2), numDwells = 1, avgPrecision = 64)
    val locIntDwells =
      Array(
        ((location1, intervals(0)), dwell1),
        ((location1, intervals(0)), dwell3),
        ((location1, intervals(0)), dwell4),
        ((location2, intervals(0)), dwell2),
        ((location2, intervals(1)), dwell2))
    val footfall =
      Array(
        ((location1, intervals(0)), footfallLoc1Time1),
        ((location2, intervals(0)), footfallLoc2Time1),
        ((location2, intervals(1)), footfallLoc2Time2))
    val profile =
      Array(
        ((location1, intervals(0)), user1),
        ((location1, intervals(0)), user3),
        ((location2, intervals(0)), user2),
        ((location2, intervals(1)), user2))
  }

  trait WithPoisForMatching {

    val location1 = Location(name = "location1", client = "client", epsg = Coordinates.SaudiArabiaUtmEpsg,
      geomWkt = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")
    val location2 = location1.copy(name = "location2", geomWkt = "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))")
    val locations = sc.parallelize(Array(location1, location2))

    val user1 = User(imei = "", imsi = "420031", msisdn = 9661)
    val user2 = User(imei = "", imsi = "420032", msisdn = 9662)
    val user3 = User(imei = "", imsi = "420033", msisdn = 9663)

    val poi1User1 = Poi(
      user = user1,
      poiType = Home,
      geomWkt = "POLYGON ((3 3, 10 3, 10 10, 3 10, 3 3))",
      countryIsoCode = CountryCode.SaudiArabiaIsoCode)
    val poi2User1 = poi1User1.copy(poiType = Work, geomWkt = "POLYGON ((7 7, 15 7, 15 15, 7 15, 7 7))")
    val poi1User2 = poi1User1.copy(user = user2, poiType = Home, geomWkt = "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))")
    val poi2User2 = poi1User1.copy(user = user2, poiType = Work, geomWkt = "POLYGON ((7 7, 11 7, 11 11, 7 11, 7 7))")
    val poi1User3 =
      poi1User1.copy(user = user3, poiType = Work, geomWkt = "POLYGON ((11 11, 15 11, 15 15, 11 15, 11 11))")
    val nonMatchingPoi = poi1User1.copy(geomWkt = "POLYGON ((30 30, 40 30, 40 40, 30 40, 30 30))")
    val pois = sc.parallelize(Array(poi1User1, poi2User1, poi1User2, poi2User2, poi1User3, nonMatchingPoi))

    val locIntPois =
      Array(
        (location1, poi1User1),
        (location1, poi1User2),
        (location1, poi2User2),
        (location2, poi2User1),
        (location2, poi1User3))
    val poiByLocation1 = LocationPoiMetrics(
        0.8541666666666666, 0.20623947784607635, 1.0, 0.5625, 2, 3, Map(Seq(Home, Work) -> 1, Seq(Home) -> 1))
    val poiByLocation2 = LocationPoiMetrics(0.6953125, 0.3046875, 1.0, 0.390625, 2, 2, Map(Seq(Work) -> 2))

    val location1Analysis = (location1, poiByLocation1)
    val location2Analysis = (location2, poiByLocation2)
  }

  trait WithLocationPoiViews extends WithPoisForMatching {

    val locationPoiView1 = LocationPoiView(location = location1, poi = poi1User1, weight = 1D)
    val locationPoiView2 = LocationPoiView(location = location1, poi = poi2User1, weight = 0.140625D)
    val locationPoiView3 = LocationPoiView(location = location1, poi = poi1User2, weight = 1D)
    val locationPoiView4 = LocationPoiView(location = location1, poi = poi2User2, weight = 0.5625D)

    val locationPoiViewsLocation1 =
      Array(locationPoiView1, locationPoiView2, locationPoiView3, locationPoiView4)

    val locationPoiView5 = LocationPoiView(location = location2, poi = poi2User1, weight = 0.390625D)
    val locationPoiView6 = LocationPoiView(location = location2, poi = poi2User2, weight = 0.0625D)
    val locationPoiView7 = LocationPoiView(location = location2, poi = poi1User3, weight = 1D)

    val locationPoiViewsLocation2 =
      Array(locationPoiView5, locationPoiView6, locationPoiView7)
  }

  trait WithItemsForMobilityMatrix {

    val startDate = EdmCoreUtils.Fmt.parseDateTime("2014/11/02 00:00:00")
    val endDate = startDate.plusHours(3)
    val intervals = EdmCoreUtils.intervals(startDate, endDate, 60)

    val itemDwell1And2 = MobilityMatrixItem(intervals(0), intervals(1), "l1", "l2", new Duration(2460000L), 1,
      User("", "4200301", 0), 1, 1)

    val dwell1 = Dwell(
      user = User("", "4200301", 0),
      startTime = 1414875600000L,
      endTime = 1414876800000L,
      geomWkt = "POLYGON ((2 6, 2 7, 3 7, 3 6, 2 6))",
      cells = Seq((2, 4), (2, 6)),
      firstEventBeginTime = 3,
      lastEventEndTime = 9,
      numEvents = 2)
    val dwell2 = dwell1.copy(
      startTime = 1414879260000L,
      endTime = 1414880460000L,
      geomWkt = "POLYGON ((6 6, 6 7, 7 7, 7 6, 6 6))")
    val dwells = sc.parallelize(Array(dwell1, dwell2)).byUserChronologically

    val l1 = Location(name = "l1", client = "client", epsg = Coordinates.SaudiArabiaUtmEpsg,
      geomWkt = "POLYGON ((1 5, 1 8, 4 8, 4 5, 1 5))")
    val l2 = l1.copy(name = "l2", geomWkt = "POLYGON ((5 5, 5 8, 8 8, 8 5, 5 5))")
    val l3 = l1.copy(name = "l3", geomWkt = "POLYGON ((0 1, 1 1, 2 1, 2 0, 0 1))")
    val l4 = l1.copy(name = "l4", geomWkt = "POLYGON ((1 2, 1 3, 2 3, 2 2, 1 2))")
    val l5 = l1.copy(name = "l5", geomWkt = "POLYGON ((3 0, 3 1, 4 1, 4 0, 3 0))")
    val l6 = l1.copy(name = "l6", geomWkt = "POLYGON ((3 2, 3 3, 4 3, 4 2, 3 2))")
    val locations = sc.parallelize(Array(l1, l2, l3, l4, l5, l6))
  }

  trait WithDwellsForDistinctUsers {

    val l1 = Location(name = "l1", client = "client", epsg = Coordinates.SaudiArabiaUtmEpsg,
      geomWkt = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")
    val l2 = l1.copy(name = "l2", geomWkt = "POLYGON ((10 10, 20 10, 20 20, 10 20, 10 10))")
    val locations = sc.parallelize(Array(l1, l2))

    val user1 = User(imei = "", imsi = "420031", msisdn = 9661)
    val user2 = User(imei = "", imsi = "420032", msisdn = 9662)
    val user3 = User(imei = "", imsi = "420033", msisdn = 9663)

    val dwell1 = Dwell(
      user = user1,
      startTime = 1000,
      endTime = 62000,
      geomWkt = "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
      cells = Seq((2, 4), (2, 6)),
      firstEventBeginTime = 3000,
      lastEventEndTime = 70000,
      numEvents = 2)
    val dwell2 = dwell1.copy(user = user2, startTime = 55000, endTime = 120000,
      geomWkt = "POLYGON ((12 12, 12 20, 20 20, 20 12, 12 12))")
    val dwell3 = dwell1.copy(startTime = 61000, endTime = 760000,
      geomWkt = "POLYGON ((50 50, 60 50, 60 60, 50 60, 50 50))")
    val dwell4 = dwell1.copy(startTime = 25812345000L, endTime = 25812445000L)
    val dwell5 = dwell1.copy(user = user3, startTime = 133000, endTime = 192000)
    val dwell6 = dwell1.copy(user = user2, startTime = 121000, endTime = 300000)

    val dwells = sc.parallelize(Array(dwell1, dwell2, dwell3, dwell4, dwell5, dwell6))
  }

  "LocationDsl" should "get correctly parsed data" in new WithLocationText {
    locationsText.toLocation.count should be (3)
  }

  it should "get errors when parsing data" in new WithLocationText {
    locationsText.toLocationErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithLocationText {
    locationsText.toParsedLocation.count should be (4)
  }

  it should "transform location geometry information to cell catalogue spatial reference system and precision model" in
    new WithLocationInWgs84 with WithCellCatalogue {
      val locationsWithTransformedGeom = locations.withTransformedGeom().collect
      locationsWithTransformedGeom.size should be (1)
      locationsWithTransformedGeom.head.epsg should be (Coordinates.SaudiArabiaUtmEpsg)
      locationsWithTransformedGeom.head.geom should equalGeometry (geom, Coordinates.OneDecimalScale)
  }

  it should "transform location geometry information to default spatial reference system and precision model" in
    new WithLocationInWgs84 with WithEmptyCellCatalogue {
      val locationsWithTransformedGeom = locations.withTransformedGeom().collect
      locationsWithTransformedGeom.size should be (1)
      locationsWithTransformedGeom.head.epsg should be (Coordinates.SaudiArabiaUtmEpsg)
      locationsWithTransformedGeom.head.geom should equalGeometry (geom, Coordinates.OneDecimalScale)
    }

  it should "get intersecting cells for each location" in new WithLocations with WithCellCatalogue {
    locations.intersectingCells.collect should
      be (Array((location1, Seq((1, 1), (1, 2), (1, 3))), (location2, Seq((1, 4))), (location3, Seq())))
  }

  it should "match dwells with some given intervals" in new WithDwellsForMatching {
    locations.matchDwell(dwells, intervals).collect should contain theSameElementsAs (locIntDwells)
  }

  it should "compute footfall per location and time interval" in new WithDwellsForMatching {
    locations.matchDwell(dwells, intervals).footfall.collect should contain theSameElementsAs (footfall)
  }

  it should "get users per location and time interval (profile)" in new WithDwellsForMatching {
    locations.matchDwell(dwells, intervals).profile.collect should contain theSameElementsAs (profile)
  }

  it should "get PoIs per location" in new WithPoisForMatching {
    locations.matchPoi(pois).collect should contain theSameElementsAs (locIntPois)
  }

  it should "calculate the poi metrics for each location" in new WithPoisForMatching {
    val analytics = locations.poiMetrics(pois)

    analytics should contain (location1Analysis)
    analytics should contain (location2Analysis)
  }

  it should "compute the mobility matrix" in new WithItemsForMobilityMatrix {
    locations.toMobilityMatrix(dwells, intervals, 15).collect should contain theSameElementsAs (List(itemDwell1And2))
  }

  it should "parse to LocationPoiView" in new WithPoisForMatching {
    locations.toLocationPoiView(pois).count should be (5)
  }

  it should "parse to LocationPoiView including weight for location 1" in new WithLocationPoiViews {
    sc.parallelize(Array(location1)).toWeightedLocationPoiView(pois).collect should contain theSameElementsAs (
      locationPoiViewsLocation1)
  }

  it should "parse to LocationPoiView including weight for location 2" in new WithLocationPoiViews {
    sc.parallelize(Array(location2)).toWeightedLocationPoiView(pois).collect should contain theSameElementsAs (
        locationPoiViewsLocation2)
  }

  it should "parse to LocationPoiView including weight for locations" in new WithLocationPoiViews {
    locations.toWeightedLocationPoiView(pois).collect should contain theSameElementsAs (
      locationPoiViewsLocation1 ++ locationPoiViewsLocation2)
  }

  it should "get distinct users per day (filtering those intersecting locations and with the minimum duration" in
    new WithDwellsForDistinctUsers {
      locations.distinctUsersPerDay(dwells, 1).collect should contain theSameElementsAs(List(user1, user2, user1))
    }

  it should "return locations with centroids" in new WithLocationsCentroids {
    locations.withCentroids.collect should be (locationsCentroids)
  }

  it should "return locations points" in new WithLocationsPointsWgs84 {
    locations.toWgs84LocationPointView.collect should contain theSameElementsAs(locationsPointsView)
  }
}
