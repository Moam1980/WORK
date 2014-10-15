/*
 * TODO: License goes here!
 */

package sa.com.mobily.utils

import scala.util.Try

/**
 * Generic utility class for External Data Monetization
 */
object EdmCoreUtils {

  val DoubleQuotesCharacter = "\""
  val SingleQuotesCharacter = "'"

  def roundAt(p: Int)(n: Double): Double = {
    // scalastyle:off magic.number
    val s = math.pow(10, p)
    // scalastyle:on magic.number
    math.round(n * s) / s
  }

  def roundAt1(n: Double): Double = roundAt(1)(n)

  def parseDouble(s: String): Option[Double] = Try { s.toDouble }.toOption

  def parseInt(s: String): Option[Int] = Try { s.toInt }.toOption

  def removeQuotes(s: String): String = {
    if (s.startsWith(DoubleQuotesCharacter) || s.startsWith(SingleQuotesCharacter))
      removeQuotes(s.substring(1))
    else if (s.endsWith(DoubleQuotesCharacter) || s.endsWith(SingleQuotesCharacter))
      removeQuotes(s.substring(0, s.length - 1))
    else s
  }
}
