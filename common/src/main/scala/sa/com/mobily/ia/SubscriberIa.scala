/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import scala.util.Try

import sa.com.mobily.parsing.{OpenCsvParser, CsvParser}
import sa.com.mobily.utils.EdmCoreUtils

case class SubscriberIa(
    subscriberId: String,
    msisdn: Long,
    genderTypeCd: String,
    prmPartyBirthDt: Option[Long],
    ethtyTypeCd: String,
    nationalityCd: String,
    occupationCd: String,
    langCd: String,
    age: Int,
    address: String,
    postalCd: String,
    uaTerminalOs: String,
    uaTerminalModel: String,
    uaBrowserName: String,
    vipNumber: String,
    maritalStatusCd: String)

object SubscriberIa extends IaParser {

  implicit val fromCsv = new CsvParser[SubscriberIa] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): SubscriberIa = {
      val Array(subscriberIdText, msisdnText, genderTypeCdText, prmPartyBirthDtText, ethtyTypeCdText,
        nationalityCdText, occupationCdText, langCdText, ageText, addressText, postalCdText,
        uaTerminalOsText, uaTerminalModelText, uaBrowserNameText, vipNumberText, maritalStatusCdText) = fields

      SubscriberIa(
        subscriberId = subscriberIdText,
        msisdn = msisdnText.toLong,
        genderTypeCd = genderTypeCdText,
        prmPartyBirthDt = parseDate(prmPartyBirthDtText),
        ethtyTypeCd = ethtyTypeCdText,
        nationalityCd = nationalityCdText,
        occupationCd = occupationCdText,
        langCd = langCdText,
        age = EdmCoreUtils.parseInt(ageText).getOrElse(0),
        address = addressText,
        postalCd = postalCdText,
        uaTerminalOs = uaTerminalOsText,
        uaTerminalModel = uaTerminalModelText,
        uaBrowserName = uaBrowserNameText,
        vipNumber = vipNumberText,
        maritalStatusCd = maritalStatusCdText)
    }
  }

  def parseDate(s: String): Option[Long] = Try { fmt.parseDateTime(s).getMillis }.toOption
}
