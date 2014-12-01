/*
 * TODO: License goes here!
 */

package sa.com.mobily.utils

import scala.collection.JavaConverters._
import scala.util.Try

import com.google.i18n.phonenumbers.PhoneNumberUtil
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}

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
  val EdmSeparator = "|"

  def roundAt(p: Int)(n: Double): Double = {
    // scalastyle:off magic.number
    val s = math.pow(10, p)
    // scalastyle:on magic.number
    math.round(n * s) / s
  }

  def roundAt1(n: Double): Double = roundAt(1)(n)

  def hexToLong(s: String): Long = java.lang.Long.parseLong(s, BaseForHexadecimal)

  def hexToInt(s: String): Int = Integer.parseInt(s, BaseForHexadecimal)

  def hexToDecimal(s: String): Option[Int] = Try { Integer.parseInt(s, BaseForHexadecimal) }.toOption

  def hexToShort(s: String): Option[Short] = Try { Integer.parseInt(s, BaseForHexadecimal).toShort }.toOption

  def parseDouble(s: String): Option[Double] = Try { s.toDouble }.toOption

  def parseFloat(s: String): Option[Float] = Try { s.toFloat }.toOption

  def parseInt(s: String): Option[Int] = Try { s.toInt }.toOption

  def parseLong(s: String): Option[Long] = Try { s.toLong }.toOption

  def parseShort(s: String): Option[Short] = Try { s.toShort }.toOption

  def parseString(s: String): Option[String] = if (parseNullString(s).isEmpty) None else Some(s)

  def doubleOption(a: Any): Option[Double] = a match {
    case a: Double => Some(a)
    case _ => None
  }

  def floatOption(a: Any): Option[Float] = a match {
    case a: Float => Some(a)
    case _ => None
  }

  def intOption(a: Any): Option[Int] = a match {
    case a: Int => Some(a)
    case _ => None
  }

  def longOption(a: Any): Option[Long] = a match {
    case a: Long => Some(a)
    case _ => None
  }

  def shortOption(a: Any): Option[Short] = a match {
    case a: Short => Some(a)
    case _ => None
  }

  def stringOption(a: Any): Option[String] = a match {
    case a: String => Some(a)
    case _ => None
  }

  def intOrZero(a: Any): Int = a match {
    case a: Int => a
    case _ => 0
  }

  def longOrZero(a: Any): Long = a match {
    case a: Long => a
    case _ => 0L
  }

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

  def roundTimestampHourly(timestamp: Long): Long =
    new DateTime(timestamp, DateTimeZone.UTC).hourOfDay.roundFloorCopy.getMillis

  def roundTimestampDaily(timestamp: Long): Long =
    new DateTime(timestamp, DateTimeZone.UTC).dayOfMonth.roundFloorCopy.getMillis

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
    case "_" => ""
    case _ => s
  }
}
