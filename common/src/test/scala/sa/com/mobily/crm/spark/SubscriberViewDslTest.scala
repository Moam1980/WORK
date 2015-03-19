/*
 * TODO: License goes here!
 */

package sa.com.mobily.crm.spark

import scala.reflect.io.File

import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.utils.LocalSparkSqlContext

class SubscriberViewDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import SubscriberViewDsl._

  trait WithSubscriberText {

    val subscriber1Imsi = "1234567"
    val subscriber2Imsi = "456789"
    val subscriber3Imsi = "9101112"
    val subscriber4Imsi = "77101112"
    val subscriberView1 = s"$subscriber1Imsi|39.75|M|SAUDI ARABIA|KSA|Pre-Paid|SamsungI930000|Voice|Retail " +
      "Customer|1367355600000|1406840400000|1406840400000|Active|100.05|S50|99.04|68.57|133.77|109.99|106.36|125.23"
    val subscriberView2 = s"$subscriber2Imsi|39.75|M|SAUDI ARABIA|KSA|Pre-Paid|SamsungI930000|Voice|Retail " +
      "Customer|1367355600000|1406840400000|1406840400000|Active|100.05|S50|99.04|68.57|133.77|109.99|106.36|125.23"
    val subscriberView3 = s"$subscriber3Imsi|39.75|M|SAUDI ARABIA|KSA|Pre-Paid|SamsungI930000|Voice|Retail " +
      "Customer|1367355600000|1406840400000|1406840400000|Active|100.05|S50|99.04|68.57|133.77|109.99|106.36|125.23"
    val subscriberView4 = s"$subscriber4Imsi|39.75|M|SAUDI ARABIA|KSA|Pre-Paid|SamsungI930000|Voice|Retail " +
      "Customer|1367355600000|1406840400000|1406840400000|Active|100.05|S50|A|68.57|133.77|109.99|106.36|125.23"
    val subscribers = sc.parallelize(List(subscriberView1, subscriberView2, subscriberView3, subscriberView4))
  }

  trait WithSubscriberRows {
    val row1 = Row("420030100040377", 39.75F, "M", Row("SAUDI ARABIA", "KSA"), Row(Row("Pre-Paid"),
      "SamsungI930000"), Row(Row("Voice"), Row("Retail Customer")), Row(1367355600000L, 1406840400000L,
      1406840400000L), Row("Active"), 100.05F, Row("S50"), Row(99.04F, 68.57F, 133.77F, 109.99F, 106.36F, 125.23F))
    val row2 = Row("420030100040377", 39.75F, "M", Row("SAUDI ARABIA", "KSA"), Row(Row("Pre-Paid"),
      "SamsungI930000"), Row(Row("Voice"), Row("Retail Customer")), Row(1367355600000L, 1406840400000L,
      1406840400000L), Row("Active"), 100.05F, Row("S50"), Row(99.04F, 68.57F, 133.77F, 109.99F, 106.36F, 125.23F))
    val rows = sc.parallelize(List(row1, row2))
  }

  "SubscriberViewDsl" should "get correctly parsed data" in new WithSubscriberText {
    subscribers.toSubscriberView.count should be (3)
  }

  it should "get errors when parsing data" in new WithSubscriberText {
    subscribers.toSubscriberViewErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithSubscriberText {
    subscribers.toParsedSubscriberView.count should be (4)
  }

  it should "save view subscribers in parquet" in new WithSubscriberText {
    val path = File.makeTemp().name
    subscribers.toSubscriberView.saveAsParquetFile(path)
    sqc.parquetFile(path).toSubscriberView.collect should contain theSameElementsAs(
      subscribers.toSubscriberView.collect())
    File(path).deleteRecursively
  }

  it should "get correctly parsed rows" in new WithSubscriberRows {
    rows.toSubscriberView.count should be (2)
  }
}
