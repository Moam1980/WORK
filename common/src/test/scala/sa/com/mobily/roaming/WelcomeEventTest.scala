/*
 * TODO: License goes here!
 */

package sa.com.mobily.roaming

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.parsing.CsvParser


class WelcomeEventTest extends FlatSpec with ShouldMatchers {

  import WelcomeEvent._

  trait WithWelcomeEvent {
    val timestamp = 1412171413000L
    val inputDateFormat = "dd.MM.yyyy HH:mm:ss"

    val welcomeEventLine = "01.10.2014 16:50:13 +966000000000 123456789012345 971000000000 0 5"
    val fields = Array("01.10.2014", "16:50:13", "+966000000000", "123456789012345", "971000000000", "0", "5")

    val welcomeEvent = WelcomeEvent(timestamp, 966000000000L, 123456789012345L, 971000000000L, OutboundSms, 5)
  }

  "WelcomeEvent" should "be built from CSV" in new WithWelcomeEvent {
    CsvParser.fromLine(welcomeEventLine).value.get should be (welcomeEvent)
  }

  it should "be discarded when the CSV format is wrong" in new WithWelcomeEvent {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(5, "WrongTech"))
  }

  it should "return correct UTC in milliseconds for a Saudi date time string" in new WithWelcomeEvent {
    WelcomeEvent.fmt.parseDateTime("01.10.2014 16:50:13").getMillis should be (timestamp)
  }

  it should "return correct input format for Saudi dates" in new WithWelcomeEvent {
    WelcomeEvent.inputDateTimeFormat should be (inputDateFormat)
  }

  it should "throw an exception if the format of the date is incorrect" in new WithWelcomeEvent {
    an [IllegalArgumentException] should be thrownBy WelcomeEvent.fmt.parseDateTime("01/10/2014 16:50:13")
  }

  it should "be built from CSV with OutboundSms scenario" in new WithWelcomeEvent {
    fromCsv.fromFields(fields.updated(5, "0")) should be (welcomeEvent.copy(scenario = OutboundSms))
  }

  it should "be built from CSV with InboundSMS scenario" in new WithWelcomeEvent {
    fromCsv.fromFields(fields.updated(5, "1")) should be (welcomeEvent.copy(scenario = InboundSms))
  }

  it should "be built from CSV with OutboundWelcomeSms scenario" in new WithWelcomeEvent {
    fromCsv.fromFields(fields.updated(5, "2")) should be (welcomeEvent.copy(scenario = OutboundWelcomeSms))
  }

  it should "be built from CSV with InboundGoodbyeSMS scenario" in new WithWelcomeEvent {
    fromCsv.fromFields(fields.updated(5, "3")) should be (welcomeEvent.copy(scenario = InboundGoodbyeSms))
  }
}
