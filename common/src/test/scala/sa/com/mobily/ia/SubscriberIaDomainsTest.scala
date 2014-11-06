/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.scalatest.{FlatSpec, ShouldMatchers}
import sa.com.mobily.parsing.CsvParser


class SubscriberIaDomainsTest extends FlatSpec with ShouldMatchers {

  import SubscriberIaDomains._

  trait WithSubscriberIaDomains {
    val timestamp = 1412110800000L
    val inputDateFormat = "yyyyMMdd"

    val subscriberDomainsLine = "\"20141001\"|\"15105283\"|\"log.advertise.1mobile.com\"|\"advertise.1mobile.com\"|" +
      "\"45\"|\"28378\"|\"20492\"|\"48870\"|\"\"|\"101\""
    val fields = Array("20141001", "15105283", "log.advertise.1mobile.com", "advertise.1mobile.com", "45", "28378",
      "20492", "48870", "", "101")

    val subscriberDomains = SubscriberIaDomains(
      timestamp = timestamp,
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
  }

  "SubscriberIaDomains" should "be built from CSV" in new WithSubscriberIaDomains {
    CsvParser.fromLine(subscriberDomainsLine).value.get should be (subscriberDomains)
  }

  it should "be discarded when the CSV format is wrong" in new WithSubscriberIaDomains {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(5, "WrongNumber"))
  }

  it should "return correct UTC in milliseconds for a Saudi date time string" in new WithSubscriberIaDomains {
    SubscriberIaDomains.fmt.parseDateTime("20141001").getMillis should be (timestamp)
  }

  it should "return correct input format for Saudi dates" in new WithSubscriberIaDomains {
    SubscriberIaDomains.inputDateTimeFormat should be (inputDateFormat)
  }

  it should "throw an exception if the format of the date is incorrect" in new WithSubscriberIaDomains {
    an [IllegalArgumentException] should be thrownBy SubscriberIaDomains.fmt.parseDateTime("01/10/2014 16:50:13")
  }
}
