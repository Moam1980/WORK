/*
 * TODO: License goes here!
 */

package sa.com.mobily.event.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
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

class EventFunctions(self: RDD[Event]) {

  def byUserChronologically: RDD[(Long, List[Event])] = self.keyBy(_.msisdn).groupByKey.map(idEvent =>
    (idEvent._1, idEvent._2.toList.sortBy(_.beginTime)))
}

trait EventDsl {

  implicit def eventReader(csv: RDD[String]): EventReader = new EventReader(csv)

  implicit def eventFunctions(events: RDD[Event]): EventFunctions = new EventFunctions(events)
}

object EventDsl extends EventDsl with ParsedItemsDsl
