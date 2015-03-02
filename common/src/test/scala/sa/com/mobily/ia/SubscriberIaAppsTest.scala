/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.parsing.CsvParser

class SubscriberIaAppsTest extends FlatSpec with ShouldMatchers {

  import SubscriberIaApps._

  trait WithSubscriberIaApps {
    val timestamp = 1412110800000L
    val inputDateFormat = "yyyyMMdd"

    val subscriberAppsLine = "\"20141001\"|\"72541945\"|\"17\"|\"58\"|\"51800\"|\"355520\"|\"407320\"|\"\"|\"101\""
    val fields = Array("20141001", "72541945", "17", "58", "51800", "355520", "407320", "", "101")

    val subscriberIaAppsFields = Array("20141001", "72541945", "17", "58", "51800.0", "355520.0", "407320.0", "", "101")
    val subscriberIaAppsHeader =
      Array("date", "subscriberId", "appType") ++ TrafficInfo.Header ++ Array("locationId", "businessEntityId")

    val row = Row(timestamp, "72541945", "17", Row(58L, 51800D, 355520D, 407320D), "", "101")
    val wrongRow = Row(timestamp, "72541945", "17", Row("NaN", 51800D, 355520D, 407320D), "", "101")
    
    val subscriberIaApps =
      SubscriberIaApps(
        timestamp = timestamp,
        subscriberId = "72541945",
        appType = "17",
        trafficInfo = TrafficInfo(
          visitCount = 58L,
          uploadVolume = 51800D,
          downloadVolume = 355520D,
          totalVolume = 407320D),
        locationId = "",
        businessEntityId = "101")
  }

  "SubscriberIaApps" should "return correct header" in new WithSubscriberIaApps {
    SubscriberIaApps.Header should be (subscriberIaAppsHeader)
  }

  it should "return correct fields" in new WithSubscriberIaApps {
    subscriberIaApps.fields should be (subscriberIaAppsFields)
  }

  it should "have same number of elements fields and header" in new WithSubscriberIaApps {
    subscriberIaApps.fields.length should be (SubscriberIaApps.Header.length)
  }
  
  it should "be built from CSV" in new WithSubscriberIaApps {
    CsvParser.fromLine(subscriberAppsLine).value.get should be (subscriberIaApps)
  }

  it should "be discarded when the CSV format is wrong" in new WithSubscriberIaApps {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(5, "WrongNumber"))
  }

  it should "return correct UTC in milliseconds for a Saudi date time string" in new WithSubscriberIaApps {
    SubscriberIaApps.Fmt.parseDateTime("20141001").getMillis should be (timestamp)
  }

  it should "return correct input format for Saudi dates" in new WithSubscriberIaApps {
    SubscriberIaApps.inputDateTimeFormat should be (inputDateFormat)
  }

  it should "throw an exception if the format of the date is incorrect" in new WithSubscriberIaApps {
    an [IllegalArgumentException] should be thrownBy SubscriberIaApps.Fmt.parseDateTime("01/10/2014 16:50:13")
  }

  it should "be built from Row" in new WithSubscriberIaApps {
    fromRow.fromRow(row) should be (subscriberIaApps)
  }

  it should "be discarded when row is wrong" in new WithSubscriberIaApps {
    an[Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }
}
