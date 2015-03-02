/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.parsing.CsvParser

class SubscriberIaTest extends FlatSpec with ShouldMatchers {

  import SubscriberIa._

  trait WithSubscriberIa {

    val timestamp = 364683600000L
    val inputDateFormat = "yyyyMMdd"

    val subscriberLine =
      "\"18711223\"|\"966560000000\"|\"1\"|\"19810723\"|\"\"|\"966\"|\"\"|\"2\"|\"33\"|\"\"|\"\"|\"\"|\"\"|\"\"|\"\"" +
        "|\"7\""
    val fields =
      Array("18711223", "966560000000", "1", "19810723", "", "966", "", "2", "33", "", "", "", "", "", "", "7")

    val subscriberIaFields =
      Array("18711223", "966560000000", "1", "19810723", "", "966", "", "2", "33", "", "", "", "", "", "", "7")
    val subscriberIaHeader =
      Array("subscriberId", "msisdn", "genderTypeCd", "prmPartyBirthDt", "ethtyTypeCd", "nationalityCd",
        "occupationCd", "langCd", "age", "address", "postalCd", "uaTerminalOs", "uaTerminalModel", "uaBrowserName",
        "vipNumber", "maritalStatusCd")

    val row = Row("18711223", 966560000000L, "1", timestamp, "", "966", "", "2", 33, "", "", "", "", "", "", "7")
    val wrongRow = Row("18711223", "NAN", "1", timestamp, "", "966", "", "2", 33, "", "", "", "", "", "", "7")

    val subscriberIa =
      SubscriberIa(
        subscriberId = "18711223",
        msisdn = 966560000000L,
        genderTypeCd = "1",
        prmPartyBirthDt = Some(timestamp),
        ethtyTypeCd = "",
        nationalityCd = "966",
        occupationCd = "",
        langCd = "2",
        age = 33,
        address = "",
        postalCd = "",
        uaTerminalOs = "",
        uaTerminalModel = "",
        uaBrowserName = "",
        vipNumber = "",
        maritalStatusCd = "7")
  }

  "SubscriberIa" should "return correct header" in new WithSubscriberIa {
    SubscriberIa.Header should be (subscriberIaHeader)
  }

  it should "return correct fields" in new WithSubscriberIa {
    subscriberIa.fields should be (subscriberIaFields)
  }

  it should "have same number of elements fields and header" in new WithSubscriberIa {
    subscriberIa.fields.length should be (SubscriberIa.Header.length)
  }

  it should "be built from CSV" in new WithSubscriberIa {
    CsvParser.fromLine(subscriberLine).value.get should be (subscriberIa)
  }

  it should "be discarded when the CSV format is wrong" in new WithSubscriberIa {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(1, "WrongNumber"))
  }

  it should "return correct UTC in milliseconds for a Saudi date time string" in new WithSubscriberIa {
    SubscriberIa.Fmt.parseDateTime("19810723").getMillis should be (timestamp)
  }

  it should "return correct input format for Saudi dates" in new WithSubscriberIa {
    SubscriberIa.inputDateTimeFormat should be (inputDateFormat)
  }

  it should "throw an exception if the format of the date is incorrect" in new WithSubscriberIa {
    an [IllegalArgumentException] should be thrownBy SubscriberIa.Fmt.parseDateTime("01/10/2014 16:50:13")
  }

  it should "return parse correct UTC in milliseconds for a Saudi date time string" in new WithSubscriberIa {
    SubscriberIa.parseDate("19810723") should be (Some(timestamp))
  }

  it should "return empty option if the format of the date is incorrect" in new WithSubscriberIa {
    SubscriberIa.parseDate("01/10/2014 16:50:13") should be (None)
  }

  it should "be built from Row" in new WithSubscriberIa {
    fromRow.fromRow(row) should be (subscriberIa)
  }

  it should "be discarded when row is wrong" in new WithSubscriberIa {
    an[Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }
}
