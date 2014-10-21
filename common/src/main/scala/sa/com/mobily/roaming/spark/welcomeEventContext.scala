/*
 * TODO: License goes here!
 */

package sa.com.mobily.roaming.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.parsing.{ParsingError, ParsedItem}
import sa.com.mobily.parsing.spark.{SparkCsvParser, ParsedItemsContext}
import sa.com.mobily.roaming.WelcomeEvent

class WelcomeEventReader(self: RDD[String]) {

  import ParsedItemsContext._

  def toParsedWelcomeEvent: RDD[ParsedItem[WelcomeEvent]] = SparkCsvParser.fromCsv[WelcomeEvent](self)

  def toWelcomeEvent: RDD[WelcomeEvent] = toParsedWelcomeEvent.values

  def toWelcomeEventErrors: RDD[ParsingError] = toParsedWelcomeEvent.errors
}

trait WelcomeEventContext {

  implicit def welcomeEventReader(csv: RDD[String]): WelcomeEventReader = new WelcomeEventReader(csv)
}

object WelcomeContext extends WelcomeEventContext with ParsedItemsContext
