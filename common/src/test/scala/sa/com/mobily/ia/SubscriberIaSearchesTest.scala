/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.scalatest.{FlatSpec, ShouldMatchers}
import sa.com.mobily.parsing.CsvParser


class SubscriberIaSearchesTest extends FlatSpec with ShouldMatchers {

  import SubscriberIaSearches._

  trait WithSubscriberIaSearches {
    val timestamp = 1412110800000L
    val inputDateFormat = "yyyyMMdd"

    val subscriberSearchesLine = "\"20141001\"|\"62182120\"|\"?????? ?????????? ???????????????????\"|" +
      "\"www.google.com.sa\"|\"1412175468\"|\"google\"|\"Web page\"|\"\"|\"101\""
    val fields = Array("20141001", "62182120", "?????? ?????????? ???????????????????", "www.google.com.sa",
      "1412175468", "google", "Web page", "", "101")

    val subscriberSearches = SubscriberIaSearches(timestamp, "62182120", "?????? ?????????? ???????????????????",
      "www.google.com.sa", 1412175468L, "google", "Web page", "", "101")
  }

  "SubscriberIaSearches" should "be built from CSV" in new WithSubscriberIaSearches {
    CsvParser.fromLine(subscriberSearchesLine).value.get should be (subscriberSearches)
  }

  it should "be discarded when the CSV format is wrong" in new WithSubscriberIaSearches {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(4, "WrongNumber"))
  }

  it should "return correct UTC in milliseconds for a Saudi date time string" in new WithSubscriberIaSearches {
    SubscriberIaSearches.fmt.parseDateTime("20141001").getMillis should be (timestamp)
  }

  it should "return correct input format for Saudi dates" in new WithSubscriberIaSearches {
    SubscriberIaSearches.inputDateTimeFormat should be (inputDateFormat)
  }

  it should "throw an exception if the format of the date is incorrect" in new WithSubscriberIaSearches {
    an [IllegalArgumentException] should be thrownBy SubscriberIaSearches.fmt.parseDateTime("01/10/2014 16:50:13")
  }
}
