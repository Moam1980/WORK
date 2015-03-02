/*
 * TODO: License goes here!
 */

package sa.com.mobily.location.spark

import scala.reflect.io.File

import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.utils.LocalSparkSqlContext

class LocationPoiViewDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import LocationPoiViewDsl._

  trait WithLocationPoiViewText {

    val poi1Imsi = "1234567"
    val poi2Imsi = "456789"
    val poi3Imsi = "9101112"
    val poi4Imsi = "77101112"
    val locationPoiView1 = s"$poi1Imsi|420|locationTest1|Home"
    val locationPoiView2 = s"$poi2Imsi|420|locationTest2|Work"
    val locationPoiView3 = s"$poi3Imsi|420|locationTest3|Home"
    val locationPoiView4 = s"$poi4Imsi|420|locationTest4"
    val locationPoiViews = sc.parallelize(List(locationPoiView1, locationPoiView2, locationPoiView3, locationPoiView4))
  }

  trait WithLocationPoiViewRows {
    val row1 = Row("420030100040377", "420", "locationTest1", Row("Home"))
    val row2 = Row("420030100040378", "420", "locationTest2", Row("Home"))
    val rows = sc.parallelize(List(row1, row2))
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
    sqc.parquetFile(path).toLocationPoiView.collect should contain
      theSameElementsAs(locationPoiViews.toLocationPoiView.collect)
    File(path).deleteRecursively
  }

  it should "get correctly parsed rows" in new WithLocationPoiViewRows {
    rows.toLocationPoiView.count should be (2)
  }
}
