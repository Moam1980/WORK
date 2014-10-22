/*
 * TODO: License goes here!
 */

package sa.com.mobily.utils

import scala.collection.JavaConverters._
import scala.util.Try

import com.google.i18n.phonenumbers.PhoneNumberUtil
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.DateTimeFormat

/**
 * Generic utility class for External Data Monetization
 */
object EdmCoreUtils {

  val outputDateTimeFormat = "yyyy/MM/dd HH:mm:ss"
  final val TimeZoneSaudiArabia = DateTimeZone.forID("Asia/Riyadh")
  final val fmt = DateTimeFormat.forPattern(outputDateTimeFormat).withZone(EdmCoreUtils.TimeZoneSaudiArabia)
  final val phoneNumberUtil = PhoneNumberUtil.getInstance

  def roundAt(p: Int)(n: Double): Double = {
    // scalastyle:off magic.number
    val s = math.pow(10, p)
    // scalastyle:on magic.number
    math.round(n * s) / s
  }

  def roundAt1(n: Double): Double = roundAt(1)(n)

  def parseDouble(s: String): Option[Double] = Try { s.toDouble }.toOption

  def parseInt(s: String): Option[Int] = Try { s.toInt }.toOption

  def getCountryCode(s: String): Int = phoneNumberUtil.parse(s, "").getCountryCode

  def getRegionCodesForCountryCode(s: String): String =
    phoneNumberUtil.getRegionCodesForCountryCode(getCountryCode(s)).asScala.mkString(":")

  def parseTimestampToSaudiDate(timestamp: Long): String = fmt.print(timestamp)

  def roundTimestampHourly(timestamp: Long): Long = new DateTime(timestamp).hourOfDay.roundFloorCopy.getMillis

  def parseYesNoBoolean(s: String): Option[Boolean] = s.toLowerCase match {
    case "y" => Some(true)
    case "yes" => Some(true)
    case "n" => Some(false)
    case "no" => Some(false)
    case _ => None
  }
}
