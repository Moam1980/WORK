/*
 * TODO: License goes here!
 */

package sa.com.mobily.crm.spark

import scala.reflect.io.File

import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.utils.LocalSparkSqlContext

class SubscriberProfilingViewDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import SubscriberProfilingViewDsl._

  trait WithSubscriberText {

    val subscriberProfilingView1 = "420030100040377|16-25|M|Saudi Arabia|Top 20%"
    val subscriberProfilingView2 = "412011234567891|26-60|F|Roamers|Middle 30%"
    val subscriberProfilingView3 = "401119876543219|60-|F|Non-Saudi|Bottom 50%"
    val subscriberProfilingView4 = "420030100040377|16-25"
    val subscribers =
      sc.parallelize(
        List(subscriberProfilingView1, subscriberProfilingView2, subscriberProfilingView3, subscriberProfilingView4))
  }

  trait WithSubscriberRows {

    val row1 = Row("420030100040377", Row("16-25", "M", "Saudi Arabia", "Top 20%"))
    val row2 = Row("412011234567891", Row("26-60", "F", "Other", "Middle 30%"))

    val rows = sc.parallelize(List(row1, row2))
  }

  "SubscriberProfilingViewDsl" should "get correctly parsed data" in new WithSubscriberText {
    subscribers.toSubscriberProfilingView.count should be (3)
  }

  it should "get errors when parsing data" in new WithSubscriberText {
    subscribers.toSubscriberProfilingViewErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithSubscriberText {
    subscribers.toParsedSubscriberProfilingView.count should be (4)
  }

  it should "save view subscribers in parquet" in new WithSubscriberText {
    val path = File.makeTemp().name
    subscribers.toSubscriberProfilingView.saveAsParquetFile(path)
    sqc.parquetFile(path).toSubscriberProfilingView.collect should contain theSameElementsAs(
      subscribers.toSubscriberProfilingView.collect())
    File(path).deleteRecursively
  }

  it should "get correctly parsed rows" in new WithSubscriberRows {
    rows.toSubscriberProfilingView.count should be (2)
  }
}
