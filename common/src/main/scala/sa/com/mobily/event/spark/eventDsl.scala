/*
 * TODO: License goes here!
 */

package sa.com.mobily.event.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.joda.time.{DateTime, Period}

import sa.com.mobily.cell.Cell
import sa.com.mobily.event._
import sa.com.mobily.flickering.{Flickering, FlickeringCells}
import sa.com.mobily.metrics.{Measurable, MetricResult, MetricResultParam}
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}
import sa.com.mobily.poi.UserActivity
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils
import sa.com.mobily.xdr.spark.{AiCsXdrDsl, IuCsXdrDsl, UfdrPsXdrDsl}

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

  import Event._

  def byUserChronologically: RDD[(User, List[Event])] = self.keyBy(_.user).groupByKey.map(userEvent =>
    (userEvent._1, userEvent._2.toList.sortBy(_.beginTime)))

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

  def toUserActivity(implicit cellCatalogue: Broadcast[Map[(Int, Int), Cell]]): RDD[UserActivity] = {
    withMatchingCell(cellCatalogue).filter(!_.user.imsi.isEmpty).flatMap(event => {
      val beginDate = new DateTime(event.beginTime, EdmCoreUtils.TimeZoneSaudiArabia).minuteOfHour().setCopy(0)
      val endDate = new DateTime(event.endTime, EdmCoreUtils.TimeZoneSaudiArabia).minuteOfHour().setCopy(0)
      val bts = cellCatalogue.value(event.lacTac, event.cellId).bts
      val hours = new Period(beginDate, endDate).getHours
      (0 to hours).map(hourToSum => {
        val dateToEmit = beginDate.plusHours(hourToSum)
        ((event.user, bts, EdmCoreUtils.regionId(event.lacTac), dateToEmit.year.get.toShort,
          EdmCoreUtils.saudiWeekOfYear(dateToEmit).toShort),
          Set(((EdmCoreUtils.saudiDayOfWeek(dateToEmit.dayOfWeek.get) - 1) * HoursInDay) + dateToEmit.hourOfDay.get))
      })
    }).reduceByKey(_ ++ _).map(keyActivityHours => {
      val key = keyActivityHours._1
      UserActivity(
        user = key._1,
        siteId = key._2,
        regionId = key._3.toShort,
        weekHoursWithActivity = keyActivityHours._2.map((_, 1D)).toMap,
        weekYear = Set((key._4, key._5)))
    })
  }
}

class EventWriter(self: RDD[Event]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[Event](self, path)
}

class EventStatistics(self: RDD[Event]) {

  def metrics: MetricResult = {
    implicit val accumulableParam = new MetricResultParam[Measurable]()
    val accumulable = self.context.accumulable(MetricResult(), "sanity")

    self.foreach(event => accumulable += event)
    accumulable.value
  }

  def saveMetrics(file: String): Unit = self.context.parallelize(metrics.toCsvFields).saveAsTextFile(file)

  def toEventsByCell: RDD[((Int, Int), Iterable[Event])] =
    self.map(event => ((event.lacTac, event.cellId), event)).groupByKey

  def countUsersByCell: RDD[((Int, Int), Int)] = toUsersByCell.map(tuple => tuple._1 -> tuple._2.size)

  def toUsersByCell: RDD[((Int, Int), Iterable[User])] = self.map(e => ((e.lacTac, e.cellId), e.user)).groupByKey

  def toEventsByCellAndUser: RDD[((Int, Int), Map[User, Int])] =
    toUsersByCell.map(event => event._1 -> event._2.foldLeft(Map[User, Int]() withDefaultValue 0) {
      (user, events) => user + (events -> (1 + user(events)))
    })
}

trait EventDsl {

  implicit def eventCsvReader(self: RDD[String]): EventCsvReader = new EventCsvReader(self)

  implicit def eventRowReader(self: RDD[Row]): EventRowReader = new EventRowReader(self)

  implicit def eventFunctions(events: RDD[Event]): EventFunctions = new EventFunctions(events)

  implicit def eventWriter(events: RDD[Event]): EventWriter = new EventWriter(events)

  implicit def eventStatistics(events: RDD[Event]): EventStatistics = new EventStatistics(events)
}

object EventDsl extends EventDsl with ParsedItemsDsl with UfdrPsXdrDsl with IuCsXdrDsl with AiCsXdrDsl
