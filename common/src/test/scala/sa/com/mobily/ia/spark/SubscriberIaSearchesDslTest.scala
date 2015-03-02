/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.reflect.io.File

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest._

import sa.com.mobily.ia.SubscriberIaSearches
import sa.com.mobily.utils.LocalSparkSqlContext

class SubscriberIaSearchesDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import SubscriberIaSearchesDsl._

  trait WithSubscriberIaSearchesText {

    val subscriberSearches1 = "\"20141001\"|\"62182120\"|\"?????? ?????????? ???????????????????\"|" +
      "\"www.google.com.sa\"|\"1412175468\"|\"google\"|\"Web page\"|\"\"|\"101\""
    val subscriberSearches2 = "\"20141001\"|\"12345678\"|\"?????? ?????????? ???????????????????\"|" +
      "\"www.yahoo.com.sa\"|\"12345678901\"|\"yahoo\"|\"Web page\"|\"LOCATION\"|\"OTHER\""
    val subscriberSearches3 = "\"20141001\"|\"12345678\"|\"?????? ?????????? ???????????????????\"|" +
      "\"www.yahoo.com.sa\"|\"text\"|\"yahoo\"|\"Web page\"|\"LOCATION\"|\"OTHER\""

    val subscriberSearches = sc.parallelize(List(subscriberSearches1, subscriberSearches2, subscriberSearches3))
  }

  trait WithSubscriberIaSearchesRows {

    val row =
      Row(1412110800000L, "62182120", "?????? ?????????? ???????????????????", "www.google.com.sa", 1412175468000L,
        "google", "Web page", "", "101")
    val row2 =
      Row(1412208000000L, "subscriberId_2", "keyword_2", "domainName_2", 1412208222000L, "engineName_2", "category_2",
        "locationId_2", "businessEntityId_2")
    val wrongRow =
      Row("NaN", "62182120", "?????? ?????????? ???????????????????", "www.google.com.sa",
        1412175468000L, "google", "Web page", "", "101")

    val rows = sc.parallelize(List(row, row2))
  }

  trait WithSubscriberIaSearches {

    val subscriberIaSearches1 =
      SubscriberIaSearches(
        timestamp = 1412110800000L,
        subscriberId = "62182120",
        keyword = "?????? ?????????? ???????????????????",
        domainName = "www.google.com.sa",
        visitDate = 1412175468000L,
        engineName = "google",
        category = "Web page",
        locationId = "",
        businessEntityId = "101")
    val subscriberIaSearches2 =
      SubscriberIaSearches(
        timestamp = 1412208000000L,
        subscriberId = "subscriberId_2",
        keyword = "keyword_2",
        domainName = "domainName_2",
        visitDate = 1412208222000L,
        engineName = "engineName_2",
        category = "category_2",
        locationId = "locationId_2",
        businessEntityId = "businessEntityId_2")
    val subscriberIaSearches3 =
      SubscriberIaSearches(
        timestamp = 1412294400000L,
        subscriberId = "subscriberId_3",
        keyword = "keyword_3",
        domainName = "domainName_3",
        visitDate = 1412294433000L,
        engineName = "engineName_3",
        category = "category_3",
        locationId = "locationId_3",
        businessEntityId = "businessEntityId_3")

    val subscriberIaSearches = sc.parallelize(List(subscriberIaSearches1, subscriberIaSearches2, subscriberIaSearches3))
  }

  "SubscriberIaSearchesDsl" should "get correctly parsed data" in new WithSubscriberIaSearchesText {
    subscriberSearches.toSubscriberIaSearches.count should be (2)
  }

  it should "get errors when parsing data" in new WithSubscriberIaSearchesText {
    subscriberSearches.toSubscriberIaSearchesErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithSubscriberIaSearchesText {
    subscriberSearches.toParsedSubscriberIaSearches.count should be (3)
  }

  it should "get correctly parsed rows" in new WithSubscriberIaSearchesRows {
    rows.toSubscriberIaSearches.count should be (2)
  }

  it should "save in parquet" in new WithSubscriberIaSearches {
    val path = File.makeTemp().name
    subscriberIaSearches.saveAsParquetFile(path)
    sqc.parquetFile(path).toSubscriberIaSearches.collect should be (subscriberIaSearches.collect)
    File(path).deleteRecursively
  }
}
