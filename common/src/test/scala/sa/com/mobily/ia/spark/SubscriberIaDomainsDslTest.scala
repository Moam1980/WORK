/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.reflect.io.File

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest._

import sa.com.mobily.ia.{SubscriberIaDomains, TrafficInfo}
import sa.com.mobily.utils.LocalSparkSqlContext

class SubscriberIaDomainsDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import SubscriberIaDomainsDsl._

  trait WithSubscriberIaDomainsText {

    val subscriberDomains1 = "\"20141001\"|\"15105283\"|\"log.advertise.1mobile.com\"|\"advertise.1mobile.com\"|" +
      "\"45\"|\"28378\"|\"20492\"|\"48870\"|\"\"|\"101\""
    val subscriberDomains2 = "\"20141001\"|\"12345678\"|\"aaaaaaaaa.aaaaaa.com.sa\"|\"advertise.aaaaa.com.sa\"|" +
      "\"77\"|\"65432\"|\"76543\"|\"98765\"|\"\"|\"OTRO\""
    val subscriberDomains3 = "\"20141001\"|\"15105283\"|\"log.advertise.1mobile.com\"|\"advertise.1mobile.com\"|" +
      "\"45\"|\"28378\"|\"20492\"|\"text\"|\"\"|\"101\""

    val subscriberDomains = sc.parallelize(List(subscriberDomains1, subscriberDomains2, subscriberDomains3))
  }

  trait WithSubscriberIaDomainsRows {

    val row =
      Row(1412110800000L, "15105283", "log.advertise.1mobile.com", "advertise.1mobile.com",
        Row(45L, 28378D, 20492D, 48870D), "", "101")
    val row2 =
      Row(1412208000000L, "22222222", "domainName_2", "secondLevelDomain_2",
        Row(20L, 55443.3D, 992043.75D, 1047487.05D), "LocId_2", "OTHER")
    val wrongRow =
      Row("NaN", "15105283", "log.advertise.1mobile.com", "advertise.1mobile.com",
        Row(45L, 28378D, 20492D, 48870D), "", "101")

    val rows = sc.parallelize(List(row, row2))
  }

  trait WithSubscriberIaDomains {

    val subscriberIaDomains1 =
      SubscriberIaDomains(
        timestamp = 1412110800000L,
        subscriberId = "15105283",
        domainName = "log.advertise.1mobile.com",
        secondLevelDomain = "advertise.1mobile.com",
        trafficInfo = TrafficInfo(
          visitCount = 45L,
          uploadVolume = 28378D,
          downloadVolume = 20492D,
          totalVolume = 48870D),
        locationId = "",
        businessEntityId = "101")
    val subscriberIaDomains2 =
      SubscriberIaDomains(
        timestamp = 1412208000000L,
        subscriberId = "22222222",
        domainName = "domainName_2",
        secondLevelDomain = "secondLevelDomain_2",
        trafficInfo = TrafficInfo(
          visitCount = 20L,
          uploadVolume = 55443.3D,
          downloadVolume = 992043.75D,
          totalVolume = 1047487.05D),
        locationId = "LocId_2",
        businessEntityId = "OTHER")
    val subscriberIaDomains3 =
      SubscriberIaDomains(
        timestamp = 1412208000000L,
        subscriberId = "3",
        domainName = "domainName_3",
        secondLevelDomain = "secondLevelDomain_3",
        trafficInfo = TrafficInfo(
          visitCount = 3L,
          uploadVolume = 3D,
          downloadVolume = 3D,
          totalVolume = 6D),
        locationId = "locationId_3",
        businessEntityId = "businessEntityId_3")

    val subscriberIaDomains = sc.parallelize(List(subscriberIaDomains1, subscriberIaDomains2, subscriberIaDomains3))
  }

  "SubscriberIaDomainsDsl" should "get correctly parsed data" in new WithSubscriberIaDomainsText {
    subscriberDomains.toSubscriberIaDomains.count should be (2)
  }

  it should "get errors when parsing data" in new WithSubscriberIaDomainsText {
    subscriberDomains.toSubscriberIaDomainsErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithSubscriberIaDomainsText {
    subscriberDomains.toParsedSubscriberIaDomains.count should be (3)
  }

  it should "get correctly parsed rows" in new WithSubscriberIaDomainsRows {
    rows.toSubscriberIaDomains.count should be (2)
  }

  it should "save in parquet" in new WithSubscriberIaDomains {
    val path = File.makeTemp().name
    subscriberIaDomains.saveAsParquetFile(path)
    sqc.parquetFile(path).toSubscriberIaDomains.collect should be (subscriberIaDomains.collect)
    File(path).deleteRecursively
  }
}
