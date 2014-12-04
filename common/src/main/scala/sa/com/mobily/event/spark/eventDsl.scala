/*
 * TODO: License goes here!
 */

package sa.com.mobily.event.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import sa.com.mobily.cell.Cell
import sa.com.mobily.event._
import sa.com.mobily.flickering.{Flickering, FlickeringCells}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}
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

  def flickeringDetector(timeWindow: Long)
      (implicit cellCatalogue: Broadcast[Map[(Int, Int), Cell]]): RDD[FlickeringCells] = {
    val byUserWithMatchingCells = withMatchingCell(cellCatalogue).map(event =>
      (event.user.msisdn, (event.beginTime, (event.lacTac, event.cellId)))).groupByKey
    byUserWithMatchingCells.flatMap(userAndTimeCell => {
      val byUserSortedCells = userAndTimeCell._2.toSeq.sortBy(timeCell => timeCell._1)
      Flickering.detect(byUserSortedCells, timeWindow)(cellCatalogue.value)
    }).distinct
  }

  def withMatchingCell(implicit cellCatalogue: Broadcast[Map[(Int, Int), Cell]]): RDD[Event] =
    self.filter(event => cellCatalogue.value.isDefinedAt((event.lacTac, event.cellId)))

  def withNonMatchingCell(implicit cellCatalogue: Broadcast[Map[(Int, Int), Cell]]): RDD[Event] =
    self.filter(event => !cellCatalogue.value.isDefinedAt((event.lacTac, event.cellId)))
}

class EventWriter(self: RDD[Event]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[Event](self, path)
}

trait EventDsl {

  implicit def eventCsvReader(self: RDD[String]): EventCsvReader = new EventCsvReader(self)

  implicit def eventRowReader(self: RDD[Row]): EventRowReader = new EventRowReader(self)

  implicit def eventFunctions(events: RDD[Event]): EventFunctions = new EventFunctions(events)

  implicit def eventWriter(events: RDD[Event]): EventWriter = new EventWriter(events)
}

object EventDsl extends EventDsl with ParsedItemsDsl
