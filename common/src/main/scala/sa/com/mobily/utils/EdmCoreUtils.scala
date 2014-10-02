/*
 * TODO: License goes here!
 */

package sa.com.mobily.utils

/**
 * Generic utility class for External Data Monetization
 */
object EdmCoreUtils {

  def roundAt(p: Int)(n: Double): Double = {
    // scalastyle:off magic.number
    val s = math.pow(10, p)
    // scalastyle:on magic.number
    math.round(n * s) / s
  }

  def roundAt1(n: Double): Double = roundAt(1)(n)
}
