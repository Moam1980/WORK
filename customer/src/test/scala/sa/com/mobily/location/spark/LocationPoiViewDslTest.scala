/*
 * TODO: License goes here!
 */

package sa.com.mobily.location.spark

import scala.reflect.io.File

import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.location.{LocationPoiView, UserPoiProfiling}
import sa.com.mobily.poi._
import sa.com.mobily.utils.LocalSparkSqlContext

class LocationPoiViewDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import LocationPoiViewDsl._

  trait WithLocationPoiViewText {

    val poi1Imsi = "1234567"
    val poi2Imsi = "456789"
    val poi3Imsi = "9101112"
    val poi4Imsi = "77101112"
    val locationPoiView1 = s"$poi1Imsi|420|locationTest1|Home|1"
    val locationPoiView2 = s"$poi2Imsi|420|locationTest2|Work|1"
    val locationPoiView3 = s"$poi3Imsi|420|locationTest3|Home|1"
    val locationPoiView4 = s"$poi4Imsi|420|locationTest4"
    val locationPoiViews = sc.parallelize(List(locationPoiView1, locationPoiView2, locationPoiView3, locationPoiView4))
  }

  trait WithLocationPoiViewRows {
    val row1 = Row("420030100040377", "420", "locationTest1", Row("Home"), 1D)
    val row2 = Row("420030100040378", "420", "locationTest2", Row("Home"), 1D)
    val rows = sc.parallelize(List(row1, row2))
  }

  trait WithUserPoiProfiling {

    val poi1Imsi = "1234567"
    val poi2Imsi = "456789"
    val poi3Imsi = "9101112"
    val poi4Imsi = "77101112"

    val locationPoiView1_1 =
      LocationPoiView(
        imsi = poi1Imsi,
        mcc = "123",
        name = "locationTest1",
        poiType = Home,
        weight = 0.23D)

    val locationPoiView1_2 =
      LocationPoiView(
        imsi = poi1Imsi,
        mcc = "123",
        name = "locationTest2",
        poiType = Work,
        weight = 0.73D)

    val locationPoiView2_1 =
      LocationPoiView(
        imsi = poi2Imsi,
        mcc = "456",
        name = "locationTest2",
        poiType = Work,
        weight = 0.45D)

    val locationPoiView2_2 =
      LocationPoiView(
        imsi = poi2Imsi,
        mcc = "456",
        name = "locationTest5",
        poiType = PoiType("Other"),
        weight = 0.825D)

    val locationPoiView3_1 =
      LocationPoiView(
        imsi = poi3Imsi,
        mcc = "910",
        name = "locationTest10",
        poiType = Home,
        weight = 0.67D)

    val locationPoiView3_2 =
      LocationPoiView(
        imsi = poi3Imsi,
        mcc = "910",
        name = "locationTest22",
        poiType = PoiType("Other"),
        weight = 0.97D)

    val locationPoiView4 =
      LocationPoiView(
        imsi = poi4Imsi,
        mcc = "771",
        name = "locationTest12",
        poiType = PoiType("Other"),
        weight = 0.12D)

    val locationPoiViews =
      sc.parallelize(List(locationPoiView1_1, locationPoiView1_2, locationPoiView2_1, locationPoiView2_2,
        locationPoiView3_1, locationPoiView3_2, locationPoiView4))

    val userPoiProfiling1 = UserPoiProfiling(poi1Imsi, HomeAndWorkDefined)
    val userPoiProfiling2 = UserPoiProfiling(poi2Imsi, WorkDefined)
    val userPoiProfiling3 = UserPoiProfiling(poi3Imsi, HomeDefined)
    val userPoiProfiling4 = UserPoiProfiling(poi4Imsi, OtherDefined)

    val userPoiProfilings =
      sc.parallelize(List(userPoiProfiling1, userPoiProfiling2, userPoiProfiling3, userPoiProfiling4))
  }

  "LocationPoiViewDsl" should "get correctly parsed data" in new WithLocationPoiViewText {
    locationPoiViews.toLocationPoiView.count should be (3)
  }

  it should "get errors when parsing data" in new WithLocationPoiViewText {
    locationPoiViews.toLocationPoiViewErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithLocationPoiViewText {
    locationPoiViews.toParsedLocationPoiView.count should be (4)
  }

  it should "save view location pois in parquet" in new WithLocationPoiViewText {
    val path = File.makeTemp().name
    locationPoiViews.toLocationPoiView.saveAsParquetFile(path)
    sqc.parquetFile(path).toLocationPoiView.collect should contain theSameElementsAs(
      locationPoiViews.toLocationPoiView.collect)
    File(path).deleteRecursively
  }

  it should "get correctly parsed rows" in new WithLocationPoiViewRows {
    rows.toLocationPoiView.count should be (2)
  }

  it should "return correct user poi profiling" in new WithUserPoiProfiling {
    locationPoiViews.toUserPoiProfiling.collect should contain theSameElementsAs(userPoiProfilings.collect)
  }
}
