/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.scalatest.{FlatSpec, ShouldMatchers}
import sa.com.mobily.parsing.CsvParser


class SubscriberIaAppsTest extends FlatSpec with ShouldMatchers {

  import SubscriberIaApps._

  trait WithSubscriberIaApps {
    val timestamp = 1412110800000L
    val inputDateFormat = "yyyyMMdd"

    val subscriberAppsLine = "\"20141001\"|\"72541945\"|\"17\"|\"58\"|\"51800\"|\"355520\"|\"407320\"|\"\"|\"101\""
    val fields = Array("20141001", "72541945", "17", "58", "51800", "355520", "407320", "", "101")

    val subscriberApps = SubscriberIaApps(timestamp, "72541945", "17", 58L, 51800D, 355520D, 407320D, "", "101")
  }

  "SubscriberIaApps" should "be built from CSV" in new WithSubscriberIaApps {
    CsvParser.fromLine(subscriberAppsLine).value.get should be (subscriberApps)
  }

  it should "be discarded when the CSV format is wrong" in new WithSubscriberIaApps {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(5, "WrongNumber"))
  }

  it should "return correct UTC in milliseconds for a Saudi date time string" in new WithSubscriberIaApps {
    SubscriberIaApps.fmt.parseDateTime("20141001").getMillis should be (timestamp)
  }

  it should "return correct input format for Saudi dates" in new WithSubscriberIaApps {
    SubscriberIaApps.inputDateTimeFormat should be (inputDateFormat)
  }

  it should "throw an exception if the format of the date is incorrect" in new WithSubscriberIaApps {
    an [IllegalArgumentException] should be thrownBy SubscriberIaApps.fmt.parseDateTime("01/10/2014 16:50:13")
  }
}
