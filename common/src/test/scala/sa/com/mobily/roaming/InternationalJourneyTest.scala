/*
 * TODO: License goes here!
 */

package sa.com.mobily.roaming

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.parsing.CsvParser

class InternationalJourneyTest extends FlatSpec with ShouldMatchers {

  import InternationalJourney._

  trait WithInternationalJourney {

    val timestamp = 1412171413000L
    val inputDateFormat = "dd.MM.yyyy HH:mm:ss"

    val internationalJourneyLine = "01.10.2014 16:50:13 +966000000000 123456789012345 971000000000 0 5"
    val fields = Array("01.10.2014", "16:50:13", "+966000000000", "123456789012345", "971000000000", "0", "5")

    val internationalJourney = InternationalJourney(timestamp, 966000000000L, 966, 971)
  }

  "InternationalJourney" should "be built from CSV" in new WithInternationalJourney {
    CsvParser.fromLine(internationalJourneyLine).value.get should be (internationalJourney)
  }

  it should "be discarded when the CSV format is wrong" in new WithInternationalJourney {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(0, "01/10/2014"))
  }

  it should "return correct UTC in milliseconds for a Saudi date time string" in new WithInternationalJourney {
    InternationalJourney.fmt.parseDateTime("01.10.2014 16:50:13").getMillis should be (timestamp)
  }

  it should "return correct input format for Saudi dates" in new WithInternationalJourney {
    InternationalJourney.inputDateTimeFormat should be (inputDateFormat)
  }

  it should "throw an exception if the format of the date is incorrect" in new WithInternationalJourney {
    an [IllegalArgumentException] should be thrownBy InternationalJourney.fmt.parseDateTime("01/10/2014 16:50:13")
  }
}
