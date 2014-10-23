/*
 * TODO: License goes here!
 */

package sa.com.mobily.event.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.event.{Event, PsEvent}
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkCsvParser}

class EventReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def psToParsedEvent: RDD[ParsedItem[Event]] = SparkCsvParser.fromCsv[Event](self)(PsEvent.fromCsv)

  def psToEventErrors: RDD[ParsingError] = psToParsedEvent.errors

  def psToEvent: RDD[Event] = psToParsedEvent.values
}

trait EventDsl {

  implicit def eventReader(self: RDD[String]): EventReader = new EventReader(self)
}

object EventDsl extends EventDsl with ParsedItemsDsl
