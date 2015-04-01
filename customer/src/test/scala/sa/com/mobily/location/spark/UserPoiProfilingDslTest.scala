/*
 * TODO: License goes here!
 */

package sa.com.mobily.location.spark

import scala.reflect.io.File

import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.utils.LocalSparkSqlContext

class UserPoiProfilingDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import UserPoiProfilingDsl._

  trait WithUserPoiProfilingText {

    val poi1Imsi = "1234567"
    val poi2Imsi = "456789"
    val poi3Imsi = "9101112"
    val poi4Imsi = "77101112"
    val userPoiProfiling1 = s"$poi1Imsi|Home & Work"
    val userPoiProfiling2 = s"$poi2Imsi|Home"
    val userPoiProfiling3 = s"$poi3Imsi|Work"
    val userPoiProfiling4 = s"$poi4Imsi|Other"

    val userPoiProfilings =
      sc.parallelize(List(userPoiProfiling1, userPoiProfiling2, userPoiProfiling3, userPoiProfiling4))
  }

  trait WithUserPoiProfilingRows {
    val row1 = Row("420030100040377", Row("Home & Work"))
    val row2 = Row("420030100040378", Row("Home"))
    val row3 = Row("420030100040379", Row("Work"))
    val row4 = Row("420030100040380", Row("Other"))

    val rows = sc.parallelize(List(row1, row2, row3, row4))
  }

  "UserPoiProfilingDsl" should "get correctly parsed data" in new WithUserPoiProfilingText {
    userPoiProfilings.toUserPoiProfiling.count should be (4)
  }

  it should "get errors when parsing data" in new WithUserPoiProfilingText {
    userPoiProfilings.toUserPoiProfilingErrors.count should be (0)
  }

  it should "get both correctly and wrongly parsed data" in new WithUserPoiProfilingText {
    userPoiProfilings.toParsedUserPoiProfiling.count should be (4)
  }

  it should "save user poi profiling in parquet" in new WithUserPoiProfilingText {
    val path = File.makeTemp().name
    userPoiProfilings.toUserPoiProfiling.saveAsParquetFile(path)
    sqc.parquetFile(path).toUserPoiProfiling.collect should contain theSameElementsAs(
      userPoiProfilings.toUserPoiProfiling.collect)
    File(path).deleteRecursively
  }

  it should "get correctly parsed rows" in new WithUserPoiProfilingRows {
    rows.toUserPoiProfiling.count should be (4)
  }
}
