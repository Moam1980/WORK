/*
 * TODO: License goes here!
 */

package sa.com.mobily.roaming

import sa.com.mobily.parsing.{OpenCsvParser, CsvParser}
import sa.com.mobily.utils.EdmCoreUtils

/** User type */
sealed trait UserType { val identifier: Int }

case object Outbound extends UserType { override val identifier = 0 }
case object Inbound extends UserType { override val identifier = 1 }

case class InternationalJourney(
    arrivalTimestamp: Long,
    msisdn: Long,
    userType: UserType,
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
        userType = parseUserType(scenarioText),
        originCountryCode = EdmCoreUtils.getCountryCallingCode(msisdnText.toLong),
        destinationCountryCode = EdmCoreUtils.getCountryCallingCode(callingNumberText.toLong))
    }
  }

  def parseUserType(scenarioText: String): UserType = scenarioText.toInt match {
    case Outbound.identifier => Outbound
    case Inbound.identifier => Inbound
  }
}
