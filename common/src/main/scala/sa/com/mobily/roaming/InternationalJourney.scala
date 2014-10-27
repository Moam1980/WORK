/*
 * TODO: License goes here!
 */

package sa.com.mobily.roaming

import sa.com.mobily.parsing.{OpenCsvParser, CsvParser}
import sa.com.mobily.utils.EdmCoreUtils

case class InternationalJourney(
    arrivalTimestamp: Long,
    msisdn: Long,
    originCountryCode: Int,
    destinationCountryCode: Int)

object InternationalJourney extends WelcomeEventParser {

  implicit val fromCsv = new CsvParser[InternationalJourney] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): InternationalJourney = {
      val Array(dateText, timeText, msisdnText, imsiText, callingNumberText, scenarioText,
        numberShortSmsSentText) = fields

      InternationalJourney(
        arrivalTimestamp = fmt.parseDateTime(dateText + " " + timeText).getMillis,
        msisdn = msisdnText.toLong,
        originCountryCode = EdmCoreUtils.getCountryCode(msisdnText),
        destinationCountryCode = EdmCoreUtils.getCountryCode("+" + callingNumberText))
    }
  }
}
