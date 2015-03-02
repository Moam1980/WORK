/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.parsing.CsvParser

class SubscriberIaDomainsTest extends FlatSpec with ShouldMatchers {

  import SubscriberIaDomains._

  trait WithSubscriberIaDomains {
    val timestamp = 1412110800000L
    val inputDateFormat = "yyyyMMdd"

    val subscriberDomainsLine =
      "\"20141001\"|\"15105283\"|\"log.advertise.1mobile.com\"|\"advertise.1mobile.com\"|\"45\"|\"28378\"|\"20492\"" +
        "|\"48870\"|\"\"|\"101\""
    val fields =
      Array("20141001", "15105283", "log.advertise.1mobile.com", "advertise.1mobile.com", "45", "28378", "20492",
        "48870", "", "101")

    val subscriberIaDomainsFields =
      Array("20141001", "15105283", "log.advertise.1mobile.com", "advertise.1mobile.com",
        "45", "28378.0", "20492.0", "48870.0", "", "101")
    val subscriberIaDomainsHeader =
      Array("date", "subscriberId", "domainName", "secondLevelDomain") ++
        TrafficInfo.Header ++
        Array("locationId", "businessEntityId")

    val row =
      Row(timestamp, "15105283", "log.advertise.1mobile.com", "advertise.1mobile.com",
        Row(45L, 28378D, 20492D, 48870D), "", "101")
    val wrongRow =
      Row("NaN", "15105283", "log.advertise.1mobile.com", "advertise.1mobile.com",
        Row(45L, 28378D, 20492D, 48870D), "", "101")
    
    val subscriberIaDomains =
      SubscriberIaDomains(
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

  "SubscriberIaDomains" should "return correct header" in new WithSubscriberIaDomains {
    SubscriberIaDomains.Header should be (subscriberIaDomainsHeader)
  }

  it should "return correct fields" in new WithSubscriberIaDomains {
    subscriberIaDomains.fields should be (subscriberIaDomainsFields)
  }

  it should "have same number of elements fields and header" in new WithSubscriberIaDomains {
    subscriberIaDomains.fields.length should be (SubscriberIaDomains.Header.length)
  }
  
  it should "be built from CSV" in new WithSubscriberIaDomains {
    CsvParser.fromLine(subscriberDomainsLine).value.get should be (subscriberIaDomains)
  }

  it should "be discarded when the CSV format is wrong" in new WithSubscriberIaDomains {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(5, "WrongNumber"))
  }

  it should "return correct UTC in milliseconds for a Saudi date time string" in new WithSubscriberIaDomains {
    SubscriberIaDomains.Fmt.parseDateTime("20141001").getMillis should be (timestamp)
  }

  it should "return correct input format for Saudi dates" in new WithSubscriberIaDomains {
    SubscriberIaDomains.inputDateTimeFormat should be (inputDateFormat)
  }

  it should "throw an exception if the format of the date is incorrect" in new WithSubscriberIaDomains {
    an [IllegalArgumentException] should be thrownBy SubscriberIaDomains.Fmt.parseDateTime("01/10/2014 16:50:13")
  }

  it should "be built from Row" in new WithSubscriberIaDomains {
    fromRow.fromRow(row) should be (subscriberIaDomains)
  }

  it should "be discarded when row is wrong" in new WithSubscriberIaDomains {
    an[Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }
}
