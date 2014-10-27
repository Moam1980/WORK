/*
 * TODO: License goes here!
 */

package sa.com.mobily.utils

import scala.collection.JavaConverters._
import scala.util.Try

import com.google.i18n.phonenumbers.PhoneNumberUtil
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

import sa.com.mobily.roaming.CountryCallingCode

/**
 * Generic utility class for External Data Monetization
 */
object EdmCoreUtils {

  val outputDateTimeFormat = "yyyy/MM/dd HH:mm:ss"
  final val TimeZoneSaudiArabia = DateTimeZone.forID("Asia/Riyadh")
  final val fmt = DateTimeFormat.forPattern(outputDateTimeFormat).withZone(EdmCoreUtils.TimeZoneSaudiArabia)
  final val phoneNumberUtil = PhoneNumberUtil.getInstance
  val BaseForHexadecimal: Int = 16

  def roundAt(p: Int)(n: Double): Double = {
    // scalastyle:off magic.number
    val s = math.pow(10, p)
    // scalastyle:on magic.number
    math.round(n * s) / s
  }

  def roundAt1(n: Double): Double = roundAt(1)(n)

  def hexToDecimal(s: String): Option[Int] = Try { Integer.parseInt(s, BaseForHexadecimal) }.toOption

  def parseDouble(s: String): Option[Double] = Try { s.toDouble }.toOption

  def parseFloat(s: String): Option[Float] = Try { s.toFloat }.toOption

  def parseInt(s: String): Option[Int] = Try { s.toInt }.toOption

  def parseLong(s: String): Option[Long] = Try { s.toLong }.toOption

  def parseShort(s: String): Option[Short] = Try { s.toShort }.toOption

  def getCountryCode(msisdn: String): Int = phoneNumberUtil.parse(msisdn, "").getCountryCode

  def getRegionCodesForCountryCodeList(msisdn: String): List[String] =
    phoneNumberUtil.getRegionCodesForCountryCode(getCountryCode(msisdn)).asScala.toList

  def getRegionCodesForCountryCode(msisdn: String): String =
    getRegionCodesForCountryCodeList(msisdn).mkString(":")

  def getCountryCallingCode(msisdn: Long): Int = {
    if (msisdn.toString.length > CountryCallingCode.maxLengthCountryCallingCode) {
      getCountryCallingCode(msisdn.toString.substring(0, CountryCallingCode.maxLengthCountryCallingCode).toLong)
    } else {
      if (CountryCallingCode.CountryCallingCodeLookup.contains(msisdn.toInt) || (msisdn.toString.length == 1)) {
        msisdn.toInt
      } else {
        getCountryCallingCode(msisdn.toString.substring(0, msisdn.toString.length - 1).toLong)
      }
    }
  }

  def parseTimestampToSaudiDate(timestamp: Long): String = fmt.print(timestamp)

  def roundTimestampHourly(timestamp: Long): Long = new DateTime(timestamp).hourOfDay.roundFloorCopy.getMillis

  def parseYesNoBooleanOption(s: String): Option[Boolean] = s.toLowerCase match {
    case "y" => Some(true)
    case "yes" => Some(true)
    case "n" => Some(false)
    case "no" => Some(false)
    case _ => None
  }

  def parseYesNoBoolean(s: String): Boolean = s.toLowerCase match {
    case "y" => true
    case "yes" => true
    case "n" => false
    case "no" => false
  }

  def parseNullString(s: String): String = s.toLowerCase match {
    case "$null$" => ""
    case _ => s
  }
}
