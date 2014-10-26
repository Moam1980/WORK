/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.parsing.CsvParser


class SubscriberIaTest extends FlatSpec with ShouldMatchers {

  import SubscriberIa._

  trait WithSubscriberIa {

    val timestamp = 364683600000L
    val inputDateFormat = "yyyyMMdd"

    val subscriberLine = "\"18711223\"|\"966560000000\"|\"1\"|\"19810723\"|\"\"|\"966\"|\"\"|\"2\"|\"33\"|\"\"|" +
      "\"\"|\"\"|\"\"|\"\"|\"\"|\"7\""
    val fields = Array("18711223", "966560000000", "1", "19810723", "", "966", "", "2", "33", "", "", "", "", "", "",
      "7")

    val subscriber = SubscriberIa("18711223", 966560000000L, "1", Some(timestamp), "", "966", "", "2", 33, "", "", "",
      "", "", "", "7")
  }

  "SubscriberIa" should "be built from CSV" in new WithSubscriberIa {
    CsvParser.fromLine(subscriberLine).value.get should be (subscriber)
  }

  it should "be discarded when the CSV format is wrong" in new WithSubscriberIa {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(1, "WrongNumber"))
  }

  it should "return correct UTC in milliseconds for a Saudi date time string" in new WithSubscriberIa {
    SubscriberIa.fmt.parseDateTime("19810723").getMillis should be (timestamp)
  }

  it should "return correct input format for Saudi dates" in new WithSubscriberIa {
    SubscriberIa.inputDateTimeFormat should be (inputDateFormat)
  }

  it should "throw an exception if the format of the date is incorrect" in new WithSubscriberIa {
    an [IllegalArgumentException] should be thrownBy SubscriberIa.fmt.parseDateTime("01/10/2014 16:50:13")
  }

  it should "return parse correct UTC in milliseconds for a Saudi date time string" in new WithSubscriberIa {
    SubscriberIa.parseDate("19810723") should be (Some(timestamp))
  }

  it should "return empty option if the format of the date is incorrect" in new WithSubscriberIa {
    SubscriberIa.parseDate("01/10/2014 16:50:13") should be (None)
  }
}
