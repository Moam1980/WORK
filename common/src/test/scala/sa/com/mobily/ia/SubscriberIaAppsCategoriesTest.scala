/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.parsing.CsvParser

class SubscriberIaAppsCategoriesTest extends FlatSpec with ShouldMatchers {

  import SubscriberIaAppsCategories._

  trait WithSubscriberIaAppsCategories {
    val timestamp = 1412110800000L
    val inputDateFormat = "yyyyMMdd"

    val subscriberAppsCategoriesLine =
      "\"20141001\"|\"12556247\"|\"18\"|\"1\"|\"3942\"|\"10976\"|\"14918\"|\"\"|\"101\""
    val fields = Array("20141001", "12556247", "18", "1", "3942", "10976", "14918", "", "101")

    val subscriberIaAppsCategoriesFields =
      Array("20141001", "12556247", "18", "1", "3942.0", "10976.0", "14918.0", "", "101")
    val subscriberIaAppsCategoriesHeader =
      Array("date", "subscriberId", "appGroup") ++ TrafficInfo.Header ++ Array("locationId", "businessEntityId")

    val row = Row(timestamp, "12556247", "18", Row(1L, 3942D, 10976D, 14918D), "", "101")
    val wrongRow = Row("NaN", "12556247", "18", Row("NaN", 51800D, 355520D, 407320D), "", "101")

    val subscriberIaAppsCategories =
      SubscriberIaAppsCategories(
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

  "SubscriberIaAppsCategories" should "return correct header" in new WithSubscriberIaAppsCategories {
    SubscriberIaAppsCategories.Header should be (subscriberIaAppsCategoriesHeader)
  }

  it should "return correct fields" in new WithSubscriberIaAppsCategories {
    subscriberIaAppsCategories.fields should be (subscriberIaAppsCategoriesFields)
  }

  it should "have same number of elements fields and header" in new WithSubscriberIaAppsCategories {
    subscriberIaAppsCategories.fields.length should be (SubscriberIaAppsCategories.Header.length)
  }

  it should "be built from CSV" in new WithSubscriberIaAppsCategories {
    CsvParser.fromLine(subscriberAppsCategoriesLine).value.get should be (subscriberIaAppsCategories)
  }

  it should "be discarded when the CSV format is wrong" in new WithSubscriberIaAppsCategories {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(5, "WrongNumber"))
  }

  it should "return correct UTC in milliseconds for a Saudi date time string" in new WithSubscriberIaAppsCategories {
    SubscriberIaAppsCategories.Fmt.parseDateTime("20141001").getMillis should be (timestamp)
  }

  it should "return correct input format for Saudi dates" in new WithSubscriberIaAppsCategories {
    SubscriberIaAppsCategories.inputDateTimeFormat should be (inputDateFormat)
  }

  it should "throw an exception if the format of the date is incorrect" in new WithSubscriberIaAppsCategories {
    an [IllegalArgumentException] should be thrownBy SubscriberIaAppsCategories.Fmt.parseDateTime("01/10/2014 16:50:13")
  }

  it should "be built from Row" in new WithSubscriberIaAppsCategories {
    fromRow.fromRow(row) should be (subscriberIaAppsCategories)
  }

  it should "be discarded when row is wrong" in new WithSubscriberIaAppsCategories {
    an[Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }
}
