/*
 * TODO: License goes here!
 */

package sa.com.mobily.roaming.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.parsing.{ParsingError, ParsedItem}
import sa.com.mobily.parsing.spark.{SparkParser, ParsedItemsDsl}
import sa.com.mobily.roaming.WelcomeEvent

class WelcomeEventReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedWelcomeEvent: RDD[ParsedItem[WelcomeEvent]] = SparkParser.fromCsv[WelcomeEvent](self)

  def toWelcomeEvent: RDD[WelcomeEvent] = toParsedWelcomeEvent.values

  def toWelcomeEventErrors: RDD[ParsingError] = toParsedWelcomeEvent.errors
}

trait WelcomeEventDsl {

  implicit def welcomeEventReader(csv: RDD[String]): WelcomeEventReader = new WelcomeEventReader(csv)
}

object WelcomeDsl extends WelcomeEventDsl with ParsedItemsDsl
