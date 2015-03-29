/*
 * TODO: License goes here!
 */

package sa.com.mobily.location.spark

import scala.reflect.io.File

import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.utils.LocalSparkSqlContext

class LocationPointViewDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import LocationPointViewDsl._

  trait WithLocationPointViewText {

    val locationPointView1 = "clientTest1|locationTest1|1|1|26.46564199|50.07687119"
    val locationPointView2 = "clientTest1|locationTest1|1|2|26.46486766|50.07820124"
    val locationPointView3 = "clientTest2|locationTest2|1|1|26.45996388|50.07880613"
    val locationPointView4 = "clientTest1|locationTest1|NaN|1|26.46564199|50.07687119"
    val locationPointView5 = "clientTest1|locationTest1|1|NaN|26.46564199|50.07687119"

    val locationPointViews =
      sc.parallelize(List(locationPointView1, locationPointView2, locationPointView3, locationPointView4,
        locationPointView5))
  }

  trait WithLocationPointViewRows {

    val row1 = Row("clientTest1", "locationTest1", 1, 1, 26.46564199D, 50.07687119D)
    val row2 = Row("clientTest1", "locationTest1", 1, 2, 26.46486766D, 50.07820124D)
    val rows = sc.parallelize(List(row1, row2))
  }

  "LocationPointViewDsl" should "get correctly parsed data" in new WithLocationPointViewText {
    locationPointViews.toLocationPointView.count should be (3)
  }

  it should "get errors when parsing data" in new WithLocationPointViewText {
    locationPointViews.toLocationPointViewErrors.count should be (2)
  }

  it should "get both correctly and wrongly parsed data" in new WithLocationPointViewText {
    locationPointViews.toParsedLocationPointView.count should be (5)
  }

  it should "save view location pois in parquet" in new WithLocationPointViewText {
    val path = File.makeTemp().name
    locationPointViews.toLocationPointView.saveAsParquetFile(path)
    sqc.parquetFile(path).toLocationPointView.collect should contain
    theSameElementsAs(locationPointViews.toLocationPointView.collect)
    File(path).deleteRecursively
  }

  it should "get correctly parsed rows" in new WithLocationPointViewRows {
    rows.toLocationPointView.count should be (2)
  }
}
