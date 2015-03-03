/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.reflect.io.File

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest._

import sa.com.mobily.ia.{SubscriberIaAppsCategories, TrafficInfo}
import sa.com.mobily.utils.LocalSparkSqlContext

class SubscriberIaAppsCategoriesDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import SubscriberIaAppsCategoriesDsl._

  trait WithSubscriberIaAppsCategoriesText {

    val subscriberAppsCategories1 = "\"20141001\"|\"12556247\"|\"18\"|\"1\"|\"3942\"|\"10976\"|\"14918\"|\"\"|\"101\""
    val subscriberAppsCategories2 = "\"20141001\"|\"12556247\"|\"444\"|\"1111\"|\"123456\"|\"9876543\"|\"14918\"|\"" +
      "\"2121|\"101\""
    val subscriberAppsCategories3 = "\"20141001\"|\"12556247\"|\"18\"|\"1\"|\"3942\"|\"text\"|\"14918\"|\"\"|\"101\""

    val subscriberAppsCategories =
      sc.parallelize(List(subscriberAppsCategories1, subscriberAppsCategories2, subscriberAppsCategories3))
  }

  trait WithSubscriberIaAppsCategoriesRows {

    val row = Row(1412110800000L, "12556247", "18", Row(1L, 3942D, 10976D, 14918D), "", "101")
    val row2 = Row(1412208000000L, "987654321", "21", Row(20L, 55443.3D, 992043.75D, 1047487.05D), "LocId_2", "OTHER")
    val wrongRow = Row("NaN", "12556247", "18", Row("NaN", 51800D, 355520D, 407320D), "", "101")

    val rows = sc.parallelize(List(row, row2))
  }

  trait WithSubscriberIaAppsCategories {

    val subscriberIaAppsCategories1 =
      SubscriberIaAppsCategories(
        timestamp = 1412110800000L,
        subscriberId = "12556247",
        appGroup = "18",
        trafficInfo = TrafficInfo(
          visitCount = 1L,
          uploadVolume = 3942D,
          downloadVolume = 10976D,
          totalVolume = 14918D),
        locationId = "",
        businessEntityId = "101")
    val subscriberIaAppsCategories2 =
      SubscriberIaAppsCategories(
        timestamp = 1412208000000L,
        subscriberId = "987654321",
        appGroup = "21",
        trafficInfo = TrafficInfo(
          visitCount = 20L,
          uploadVolume = 55443.3D,
          downloadVolume = 992043.75D,
          totalVolume = 1047487.05D),
        locationId = "LocId_2",
        businessEntityId = "OTHER")
    val subscriberIaAppsCategories3 =
      SubscriberIaAppsCategories(
        timestamp = 1412294400000L,
        subscriberId = "33333333",
        appGroup = "3",
        trafficInfo = TrafficInfo(
          visitCount = 3L,
          uploadVolume = 3D,
          downloadVolume = 3D,
          totalVolume = 6D),
        locationId = "locationId_3",
        businessEntityId = "businessEntityId_3")

    val subscriberIaAppsCategories =
      sc.parallelize(List(subscriberIaAppsCategories1, subscriberIaAppsCategories2, subscriberIaAppsCategories3))
  }
  
  "SubscriberIaAppsCategoriesDsl" should "get correctly parsed data" in new WithSubscriberIaAppsCategoriesText {
    subscriberAppsCategories.toSubscriberIaAppsCategories.count should be (2)
  }

  it should "get errors when parsing data" in new WithSubscriberIaAppsCategoriesText {
    subscriberAppsCategories.toSubscriberIaAppsCategoriesErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithSubscriberIaAppsCategoriesText {
    subscriberAppsCategories.toParsedSubscriberIaAppsCategories.count should be (3)
  }

  it should "get correctly parsed rows" in new WithSubscriberIaAppsCategoriesRows {
    rows.toSubscriberIaAppsCategories.count should be (2)
  }

  it should "save in parquet" in new WithSubscriberIaAppsCategories {
    val path = File.makeTemp().name
    subscriberIaAppsCategories.saveAsParquetFile(path)
    sqc.parquetFile(path).toSubscriberIaAppsCategories.collect should be (subscriberIaAppsCategories.collect)
    File(path).deleteRecursively
  }
}
