/*
 * TODO: License goes here!
 */

package sa.com.mobily.roaming

import org.joda.time.format.DateTimeFormat

import sa.com.mobily.parsing.{OpenCsvParser, CsvParser}
import sa.com.mobily.utils.EdmCoreUtils

/** Scenario of the welcome SMS */
sealed trait Scenario { val identifier: Int }

case object OutboundSms extends Scenario { override val identifier = 0 }
case object InboundSms extends Scenario { override val identifier = 1 }
case object OutboundWelcomeSms extends Scenario { override val identifier = 2 }
case object InboundGoodbyeSms extends Scenario { override val identifier = 3 }

case class WelcomeEvent(
    timestamp: Long,
    msisdn: Long,
    imsi: Long,
    callingNumber: Long,
    scenario: Scenario,
    numberShortSmsSent: Int)

trait WelcomeEventParser {

  val inputDateTimeFormat = "dd.MM.yyyy HH:mm:ss"
  final val fmt = DateTimeFormat.forPattern(inputDateTimeFormat).withZone(EdmCoreUtils.TimeZoneSaudiArabia)
  final val lineCsvParserObject = new OpenCsvParser(separator = ' ')
}

object WelcomeEvent extends WelcomeEventParser {

  implicit val fromCsv = new CsvParser[WelcomeEvent] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): WelcomeEvent = {
      val Array(dateText, timeText, msisdnText, imsiText, callingNumberText, scenarioText,
        numberShortSmsSentText) = fields

      // Try to get region for msisdn and calling number
      EdmCoreUtils.getRegionCodesForCountryCode(msisdnText)
      EdmCoreUtils.getRegionCodesForCountryCode("+" + callingNumberText)

      WelcomeEvent(
        timestamp = fmt.parseDateTime(dateText + " " + timeText).getMillis,
        msisdn = msisdnText.toLong,
        imsi = imsiText.toLong,
        callingNumber = callingNumberText.toLong,
        scenario = WelcomeEvent.parseScenario(scenarioText),
        numberShortSmsSent = numberShortSmsSentText.toInt)
    }
  }

  def parseScenario(scenarioText: String): Scenario = scenarioText.toInt match {
    case OutboundSms.identifier => OutboundSms
    case InboundSms.identifier => InboundSms
    case OutboundWelcomeSms.identifier => OutboundWelcomeSms
    case InboundGoodbyeSms.identifier => InboundGoodbyeSms
  }
}
