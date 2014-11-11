/*
 * TODO: License goes here!
 */

package sa.com.mobily.event.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import sa.com.mobily.event._
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser}
import sa.com.mobily.parsing.{ParsedItem, ParsingError}

class EventCsvReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def psToParsedEvent: RDD[ParsedItem[Event]] = SparkParser.fromCsv[Event](self)(PsEvent.fromCsv)

  def psToEventErrors: RDD[ParsingError] = psToParsedEvent.errors

  def psToEvent: RDD[Event] = psToParsedEvent.values

  def voiceToParsedEvent: RDD[ParsedItem[Event]] = SparkParser.fromCsv[Event](self)(VoiceEvent.fromCsv)

  def voiceToEventErrors: RDD[ParsingError] = voiceToParsedEvent.errors

  def voiceToEvent: RDD[Event] = voiceToParsedEvent.values

  def smsToParsedEvent: RDD[ParsedItem[Event]] = SparkParser.fromCsv[Event](self)(SmsEvent.fromCsv)

  def smsToEventErrors: RDD[ParsingError] = smsToParsedEvent.errors

  def smsToEvent: RDD[Event] = smsToParsedEvent.values
}

class EventRowReader(self: RDD[Row]) {

  def toEvent: RDD[Event] = SparkParser.fromRow[Event](self)
}

class EventFunctions(self: RDD[Event]) {

  def byUserChronologically: RDD[(Long, List[Event])] = self.keyBy(_.user.msisdn).groupByKey.map(idEvent =>
    (idEvent._1, idEvent._2.toList.sortBy(_.beginTime)))
}

trait EventDsl {

  implicit def eventCsvReader(self: RDD[String]): EventCsvReader = new EventCsvReader(self)

  implicit def eventRowReader(self: RDD[Row]): EventRowReader = new EventRowReader(self)

  implicit def eventFunctions(events: RDD[Event]): EventFunctions = new EventFunctions(events)
}

object EventDsl extends EventDsl with ParsedItemsDsl
