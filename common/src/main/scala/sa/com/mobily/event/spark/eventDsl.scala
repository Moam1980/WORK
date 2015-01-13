/*
 * TODO: License goes here!
 */

package sa.com.mobily.event.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.joda.time.{Period, DateTime}

import sa.com.mobily.cell.Cell
import sa.com.mobily.event._
import sa.com.mobily.flickering.{Flickering, FlickeringCells}
import sa.com.mobily.metrics.{Measurable, MetricResult, MetricResultParam}
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}
import sa.com.mobily.poi.UserActivity
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils

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

  def toUserActivity(implicit cellCatalogue: Broadcast[Map[(Int, Int), Cell]]): RDD[UserActivity] = {
    val byUserWeekYearRegion = withMatchingCell(cellCatalogue).flatMap(event => {
      val beginDate = new DateTime(event.beginTime, EdmCoreUtils.TimeZoneSaudiArabia).minuteOfHour().setCopy(0)
      val endDate = new DateTime(event.endTime, EdmCoreUtils.TimeZoneSaudiArabia).minuteOfHour().setCopy(0)
      val hours = new Period(beginDate, endDate).getHours
      (0 to hours).map(hourToSum => {
        val dateToEmit = beginDate.plusHours(hourToSum)
        val bts = cellCatalogue.value(event.lacTac, event.cellId).bts
        ((event.user, bts, event.regionId),
          (EdmCoreUtils.saudiDayOfWeek(dateToEmit.dayOfWeek.get), dateToEmit.hourOfDay.get))
      })
    }).groupByKey
    byUserWeekYearRegion.map(keyAndActivityByWeek => {
      val activityByWeek = keyAndActivityByWeek._2
      val key = keyAndActivityByWeek._1
      val activityHoursByWeek = activityByWeek.map(dayHour => (((dayHour._1 - 1) * HoursInDay) + dayHour._2, 1D)).toSeq
      UserActivity(
        user = key._1,
        siteId = key._2,
        regionId = key._3,
        activityVector = Vectors.sparse(HoursInWeek, activityHoursByWeek))
    })
  }

  def perUserAndSiteIdFilteringLittleActivity(
    minimumActivityRatio: Double = DefaultMinActivityRatio)
    (implicit cellCatalogue: Broadcast[Map[(Int, Int), Cell]]): RDD[UserActivity] = {
    val minNumberOfHours = HoursInWeek * minimumActivityRatio
    toUserActivity(cellCatalogue).filter(
      element => element.activityVector.toArray.count(element => element == 1) > minNumberOfHours)
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

object EventDsl extends EventDsl with ParsedItemsDsl
