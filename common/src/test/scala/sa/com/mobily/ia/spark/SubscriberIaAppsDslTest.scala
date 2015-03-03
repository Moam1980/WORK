/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.reflect.io.File

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest._

import sa.com.mobily.ia.{SubscriberIaApps, TrafficInfo}
import sa.com.mobily.utils.LocalSparkSqlContext

class SubscriberIaAppsDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import SubscriberIaAppsDsl._

  trait WithSubscriberIaAppsText {

    val subscriberApps1 = "\"20141001\"|\"72541945\"|\"17\"|\"58\"|\"51800\"|\"355520\"|\"407320\"|\"\"|\"101\""
    val subscriberApps2 = "\"20141001\"|\"12345678\"|\"30\"|\"669\"|\"12345\"|\"654321\"|\"987655\"|\"\"|\"203\""
    val subscriberApps3 = "\"20141001\"|\"12345678\"|\"30\"|\"669\"|\"text\"|\"654321\"|\"987655\"|\"\"|\"203\""

    val subscriberApps = sc.parallelize(List(subscriberApps1, subscriberApps2, subscriberApps3))
  }

  trait WithSubscriberIaAppsRows {

    val row = Row(1412110800000L, "72541945", "17", Row(58L, 51800D, 355520D, 407320D), "", "101")
    val row2 = Row(1412110800000L, "22222222", "22", Row(20L, 55443.3D, 992043.75D, 1047487.05D), "LocId_2", "OTHER")
    val wrongRow = Row(1412294400000L, "72541945", "17", Row("NaN", 51800D, 355520D, 407320D), "", "101")

    val rows = sc.parallelize(List(row, row2))
  }

  trait WithSubscriberIaApps {

    val subscriberIaApps1 =
      SubscriberIaApps(
        timestamp = 1412110800000L,
        subscriberId = "72541945",
        appType = "17",
        trafficInfo = TrafficInfo(
          visitCount = 58L,
          uploadVolume = 51800D,
          downloadVolume = 355520D,
          totalVolume = 407320D),
        locationId = "",
        businessEntityId = "101")
    val subscriberIaApps2 =
      SubscriberIaApps(
        timestamp = 1412110800000L,
        subscriberId = "22222222",
        appType = "22",
        trafficInfo = TrafficInfo(
          visitCount = 20L,
          uploadVolume = 55443.3D,
          downloadVolume = 992043.75D,
          totalVolume = 1047487.05D),
        locationId = "LocId_2",
        businessEntityId = "OTHER")
    val subscriberIaApps3 =
      SubscriberIaApps(
        timestamp = 1412294400000L,
        subscriberId = "333333",
        appType = "3",
        trafficInfo = TrafficInfo(
          visitCount = 3L,
          uploadVolume = 3D,
          downloadVolume = 3D,
          totalVolume = 6D),
        locationId = "locationId_3",
        businessEntityId = "businessEntityId_3")

    val subscriberIaApps =
      sc.parallelize(List(subscriberIaApps1, subscriberIaApps2, subscriberIaApps3))
  }

  "SubscriberIaAppsDsl" should "get correctly parsed data" in new WithSubscriberIaAppsText {
    subscriberApps.toSubscriberIaApps.count should be (2)
  }

  it should "get errors when parsing data" in new WithSubscriberIaAppsText {
    subscriberApps.toSubscriberIaAppsErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithSubscriberIaAppsText {
    subscriberApps.toParsedSubscriberIaApps.count should be (3)
  }

  it should "get correctly parsed rows" in new WithSubscriberIaAppsRows {
    rows.toSubscriberIaApps.count should be (2)
  }

  it should "save in parquet" in new WithSubscriberIaApps {
    val path = File.makeTemp().name
    subscriberIaApps.saveAsParquetFile(path)
    sqc.parquetFile(path).toSubscriberIaApps.collect should be (subscriberIaApps.collect)
    File(path).deleteRecursively
  }
}
