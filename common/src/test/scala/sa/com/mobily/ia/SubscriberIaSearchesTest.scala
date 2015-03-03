/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.parsing.CsvParser

class SubscriberIaSearchesTest extends FlatSpec with ShouldMatchers {

  import SubscriberIaSearches._

  trait WithSubscriberIaSearches {
    val timestamp = 1412110800000L
    val inputDateFormat = "yyyyMMdd"

    val subscriberSearchesLine =
      "\"20141001\"|\"62182120\"|\"?????? ?????????? ???????????????????\"|\"www.google.com.sa\"|\"1412175468\"" +
        "|\"google\"|\"Web page\"|\"\"|\"101\""
    val fields =
      Array("20141001", "62182120", "?????? ?????????? ???????????????????", "www.google.com.sa", "1412175468",
        "google", "Web page", "", "101")

    val subscriberIaSearchesFields =
      Array("20141001", "62182120", "?????? ?????????? ???????????????????", "www.google.com.sa",
        "20141001", "google", "Web page", "", "101")
    val subscriberIaSearchesHeader =
      Array("date", "subscriberId", "keyword", "domainName", "visitDate", "engineName", "category",
        "locationId", "businessEntityId")

    val row =
      Row(timestamp, "62182120", "?????? ?????????? ???????????????????", "www.google.com.sa",
        1412175468000L, "google", "Web page", "", "101")
    val wrongRow =
      Row("NaN", "62182120", "?????? ?????????? ???????????????????", "www.google.com.sa",
        1412175468000L, "google", "Web page", "", "101")

    val subscriberIaSearches =
      SubscriberIaSearches(
        timestamp = timestamp,
        subscriberId = "62182120",
        keyword = "?????? ?????????? ???????????????????",
        domainName = "www.google.com.sa",
        visitDate = 1412175468000L,
        engineName = "google",
        category = "Web page",
        locationId = "",
        businessEntityId = "101")
  }
  
  "SubscriberIaSearches" should "return correct header" in new WithSubscriberIaSearches {
    SubscriberIaSearches.Header should be (subscriberIaSearchesHeader)
  }

  it should "return correct fields" in new WithSubscriberIaSearches {
    subscriberIaSearches.fields should be (subscriberIaSearchesFields)
  }

  it should "have same number of elements fields and header" in new WithSubscriberIaSearches {
    subscriberIaSearches.fields.length should be (SubscriberIaSearches.Header.length)
  }
  
  it should "be built from CSV" in new WithSubscriberIaSearches {
    CsvParser.fromLine(subscriberSearchesLine).value.get should be (subscriberIaSearches)
  }

  it should "be discarded when the CSV format is wrong" in new WithSubscriberIaSearches {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(4, "WrongNumber"))
  }

  it should "return correct UTC in milliseconds for a Saudi date time string" in new WithSubscriberIaSearches {
    SubscriberIaSearches.Fmt.parseDateTime("20141001").getMillis should be (timestamp)
  }

  it should "return correct input format for Saudi dates" in new WithSubscriberIaSearches {
    SubscriberIaSearches.inputDateTimeFormat should be (inputDateFormat)
  }

  it should "throw an exception if the format of the date is incorrect" in new WithSubscriberIaSearches {
    an [IllegalArgumentException] should be thrownBy SubscriberIaSearches.Fmt.parseDateTime("01/10/2014 16:50:13")
  }

  it should "be built from Row" in new WithSubscriberIaSearches {
    fromRow.fromRow(row) should be (subscriberIaSearches)
  }

  it should "be discarded when row is wrong" in new WithSubscriberIaSearches {
    an[Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }
}
