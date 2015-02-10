/*
 * TODO: License goes here!
 */

package sa.com.mobily.utils

object SanityUtils {

  def perform[A](properties: List[(String, A)], f: (A => Boolean)): List[(String, Int)] =
    properties.collect { case e if f(e._2) => (e._1, 1) }
}
