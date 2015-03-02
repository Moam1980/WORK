/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import scala.util.Try

import org.apache.spark.sql._

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}
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
    maritalStatusCd: String) {

  def fields: Array[String] =
    Array(subscriberId,
      msisdn.toString,
      genderTypeCd,
      prmPartyBirthDt.map(date => SubscriberIa.Fmt.print(date)).getOrElse(""),
      ethtyTypeCd,
      nationalityCd,
      occupationCd,
      langCd,
      age.toString,
      address,
      postalCd,
      uaTerminalOs,
      uaTerminalModel,
      uaBrowserName,
      vipNumber,
      maritalStatusCd)
}

object SubscriberIa extends IaParser {

  val Header: Array[String] =
    Array("subscriberId", "msisdn", "genderTypeCd", "prmPartyBirthDt", "ethtyTypeCd", "nationalityCd", "occupationCd",
      "langCd", "age", "address", "postalCd", "uaTerminalOs", "uaTerminalModel", "uaBrowserName", "vipNumber",
      "maritalStatusCd")

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

  def parseDate(s: String): Option[Long] = Try { Fmt.parseDateTime(s).getMillis }.toOption

  implicit val fromRow = new RowParser[SubscriberIa] {

    override def fromRow(row: Row): SubscriberIa = {
      val Row(
        subscriberId,
        msisdn,
        genderTypeCd,
        prmPartyBirthDt,
        ethtyTypeCd,
        nationalityCd,
        occupationCd,
        langCd,
        age,
        address,
        postalCd,
        uaTerminalOs,
        uaTerminalModel,
        uaBrowserName,
        vipNumber,
        maritalStatusCd) = row

      SubscriberIa(
        subscriberId = subscriberId.asInstanceOf[String],
        msisdn = msisdn.asInstanceOf[Long],
        genderTypeCd = genderTypeCd.asInstanceOf[String],
        prmPartyBirthDt = EdmCoreUtils.longOption(prmPartyBirthDt),
        ethtyTypeCd = ethtyTypeCd.asInstanceOf[String],
        nationalityCd = nationalityCd.asInstanceOf[String],
        occupationCd = occupationCd.asInstanceOf[String],
        langCd = langCd.asInstanceOf[String],
        age = age.asInstanceOf[Int],
        address = address.asInstanceOf[String],
        postalCd = postalCd.asInstanceOf[String],
        uaTerminalOs = uaTerminalOs.asInstanceOf[String],
        uaTerminalModel = uaTerminalModel.asInstanceOf[String],
        uaBrowserName = uaBrowserName.asInstanceOf[String],
        vipNumber = vipNumber.asInstanceOf[String],
        maritalStatusCd = maritalStatusCd.asInstanceOf[String])
    }
  }
}
