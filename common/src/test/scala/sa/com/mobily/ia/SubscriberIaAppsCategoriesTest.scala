/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.scalatest.{FlatSpec, ShouldMatchers}
import sa.com.mobily.parsing.CsvParser


class SubscriberIaAppsCategoriesTest extends FlatSpec with ShouldMatchers {

  import SubscriberIaAppsCategories._

  trait WithSubscriberIaAppsCategories {
    val timestamp = 1412110800000L
    val inputDateFormat = "yyyyMMdd"

    val subscriberAppsCategoriesLine = "\"20141001\"|\"12556247\"|\"18\"|\"1\"|\"3942\"|\"10976\"|\"14918\"|\"\"|" +
      "\"101\""
    val fields = Array("20141001", "12556247", "18", "1", "3942", "10976", "14918", "", "101")

    val subscriberAppsCategories = SubscriberIaAppsCategories(
      timestamp = timestamp,
      subscriberId = "12556247",
      appGroup = "18",
      trafficInfo = TrafficInfo(
        visitCount = 1L,
        uploadVolume = 3942D,
        downloadVolume = 10976D,
        totalVolume = 14918D),
      locationId = "",
      businessEntityId = "101")
  }

  "SubscriberIaAppsCategories" should "be built from CSV" in new WithSubscriberIaAppsCategories {
    CsvParser.fromLine(subscriberAppsCategoriesLine).value.get should be (subscriberAppsCategories)
  }

  it should "be discarded when the CSV format is wrong" in new WithSubscriberIaAppsCategories {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(5, "WrongNumber"))
  }

  it should "return correct UTC in milliseconds for a Saudi date time string" in new WithSubscriberIaAppsCategories {
    SubscriberIaAppsCategories.fmt.parseDateTime("20141001").getMillis should be (timestamp)
  }

  it should "return correct input format for Saudi dates" in new WithSubscriberIaAppsCategories {
    SubscriberIaAppsCategories.inputDateTimeFormat should be (inputDateFormat)
  }

  it should "throw an exception if the format of the date is incorrect" in new WithSubscriberIaAppsCategories {
    an [IllegalArgumentException] should be thrownBy SubscriberIaAppsCategories.fmt.parseDateTime("01/10/2014 16:50:13")
  }
}
