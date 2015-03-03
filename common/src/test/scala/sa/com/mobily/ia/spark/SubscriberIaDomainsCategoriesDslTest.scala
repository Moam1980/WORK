/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.reflect.io.File

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest._

import sa.com.mobily.ia.{SubscriberIaDomainsCategories, TrafficInfo}
import sa.com.mobily.utils.LocalSparkSqlContext

class SubscriberIaDomainsCategoriesDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import SubscriberIaDomainsCategoriesDsl._

  trait WithSubscriberIaDomainsCategoriesText {

    val subscriberDomainsCategories1 = "\"20141001\"|\"87865881\"|\"100120\"|\"933\"|\"663082\"|\"15365627\"|" +
      "\"16028709\"|\"\"|\"101\""
    val subscriberDomainsCategories2 = "\"20141001\"|\"12345678\"|\"888999\"|\"444\"|\"654321\"|\"87654321\"|" +
      "\"98765432\"|\"EXAMPLE\"|\"OTHER\""
    val subscriberDomainsCategories3 = "\"20141001\"|\"87865881\"|\"100120\"|\"933\"|\"text\"|\"15365627\"|" +
      "\"16028709\"|\"\"|\"101\""

    val subscriberDomainsCategories = sc.parallelize(List(subscriberDomainsCategories1, subscriberDomainsCategories2,
      subscriberDomainsCategories3))
  }

  trait WithSubscriberIaDomainsCategoriesRows {

    val row = Row(1412110800000L, "87865881", "100120", Row(933L, 663082D, 15365627D, 16028709D), "", "101")
    val row2 = Row(1412208000000L, "22222222", "2", Row(20L, 55443.3D, 992043.75D, 1047487.05D), "LocId_2", "OTHER")
    val wrongRow = Row("NaN", "87865881", "100120", Row(933L, 663082D, 15365627D, 16028709D), "", "101")

    val rows = sc.parallelize(List(row, row2))
  }

  trait WithSubscriberIaDomainsCategories {

    val subscriberIaDomainsCategories1 =
      SubscriberIaDomainsCategories(
        timestamp = 1412110800000L,
        subscriberId = "87865881",
        categoryId = "100120",
        trafficInfo = TrafficInfo(
          visitCount = 933L,
          uploadVolume = 663082D,
          downloadVolume = 15365627D,
          totalVolume = 16028709D),
        locationId = "",
        businessEntityId = "101")
    val subscriberIaDomainsCategories2 =
      SubscriberIaDomainsCategories(
        timestamp = 1412208000000L,
        subscriberId = "22222222",
        categoryId = "2",
        trafficInfo = TrafficInfo(
          visitCount = 20L,
          uploadVolume = 55443.3D,
          downloadVolume = 992043.75D,
          totalVolume = 1047487.05D),
        locationId = "LocId_2",
        businessEntityId = "OTHER")
    val subscriberIaDomainsCategories3 =
      SubscriberIaDomainsCategories(
        timestamp = 1412208000000L,
        subscriberId = "3",
        categoryId = "3",
        trafficInfo = TrafficInfo(
          visitCount = 3L,
          uploadVolume = 3D,
          downloadVolume = 3D,
          totalVolume = 6D),
        locationId = "locationId_3",
        businessEntityId = "businessEntityId_3")

    val subscriberIaDomainsCategories =
      sc.parallelize(
        List(subscriberIaDomainsCategories1, subscriberIaDomainsCategories2, subscriberIaDomainsCategories3))
  }

  "SubscriberIaDomainsCategoriesDsl" should "get correctly parsed data" in new WithSubscriberIaDomainsCategoriesText {
    subscriberDomainsCategories.toSubscriberIaDomainsCategories.count should be (2)
  }

  it should "get errors when parsing data" in new WithSubscriberIaDomainsCategoriesText {
    subscriberDomainsCategories.toSubscriberIaDomainsCategoriesErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithSubscriberIaDomainsCategoriesText {
    subscriberDomainsCategories.toParsedSubscriberIaDomainsCategories.count should be (3)
  }

  it should "get correctly parsed rows" in new WithSubscriberIaDomainsCategoriesRows {
    rows.toSubscriberIaDomainsCategories.count should be (2)
  }

  it should "save in parquet" in new WithSubscriberIaDomainsCategories {
    val path = File.makeTemp().name
    subscriberIaDomainsCategories.saveAsParquetFile(path)
    sqc.parquetFile(path).toSubscriberIaDomainsCategories.collect should be (subscriberIaDomainsCategories.collect)
    File(path).deleteRecursively
  }
}
