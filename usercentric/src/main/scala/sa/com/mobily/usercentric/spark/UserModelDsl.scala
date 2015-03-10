/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.util.StatCounter

import sa.com.mobily.cell.Cell
import sa.com.mobily.cell.spark.CellDsl
import sa.com.mobily.event.Event
import sa.com.mobily.event.spark.EventDsl
import sa.com.mobily.geometry.{Coordinates, GeomUtils}
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}
import sa.com.mobily.user.User
import sa.com.mobily.usercentric._

class DwellReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedDwell: RDD[ParsedItem[Dwell]] = SparkParser.fromCsv[Dwell](self)

  def toDwell: RDD[Dwell] = toParsedDwell.values

  def toDwellErrors: RDD[ParsingError] = toParsedDwell.errors
}

class JourneyReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedJourney: RDD[ParsedItem[Journey]] = SparkParser.fromCsv[Journey](self)

  def toJourney: RDD[Journey] = toParsedJourney.values

  def toJourneyErrors: RDD[ParsingError] = toParsedJourney.errors
}

class JourneyViaPointReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedJourneyViaPoint: RDD[ParsedItem[JourneyViaPoint]] = SparkParser.fromCsv[JourneyViaPoint](self)

  def toJourneyViaPoint: RDD[JourneyViaPoint] = toParsedJourneyViaPoint.values

  def toJourneyViaPointErrors: RDD[ParsingError] = toParsedJourneyViaPoint.errors
}

class DwellRowReader(self: RDD[Row]) {

  def toDwell: RDD[Dwell] = SparkParser.fromRow[Dwell](self)
}

class JourneyRowReader(self: RDD[Row]) {

  def toJourney: RDD[Journey] = SparkParser.fromRow[Journey](self)
}

class JourneyViaPointRowReader(self: RDD[Row]) {

  def toJourneyViaPoint: RDD[JourneyViaPoint] = SparkParser.fromRow[JourneyViaPoint](self)
}

class DwellWriter(self: RDD[Dwell]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[Dwell](self, path)
}

class JourneyWriter(self: RDD[Journey]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[Journey](self, path)
}

class JourneyViaPointWriter(self: RDD[JourneyViaPoint]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[JourneyViaPoint](self, path)
}

class UserModelEventFunctions(userEventsWithMatchingCell: RDD[(User, List[Event])]) {

  def aggTemporalOverlapAndSameCell(
      implicit cellCatalogue: Broadcast[Map[(Int, Int), Cell]]): RDD[(User, List[SpatioTemporalSlot])] =
    userEventsWithMatchingCell.mapValues(
      events => UserModel.aggTemporalOverlapAndSameCell(events)(cellCatalogue.value))
}

class UserModelSlotFunctions(userSlots: RDD[(User, List[SpatioTemporalSlot])]) {

  def combine(implicit cellCatalogue: Broadcast[Map[(Int, Int), Cell]]): RDD[(User, List[SpatioTemporalSlot])] =
    userSlots.mapValues(slots => {
      val slotsWithScores = UserModel.computeScores(slots)(cellCatalogue.value)
      UserModel.aggregateCompatible(slotsWithScores)(cellCatalogue.value)
    })

  def toUserCentric(implicit cellCatalogue: Broadcast[Map[(Int, Int), Cell]]):
      RDD[(User, (List[Dwell], List[Journey], List[JourneyViaPoint]))] = {
    combine.mapValues(slots => {
      val withViaPoints = UserModel.fillViaPoints(slots)(cellCatalogue.value)
      val withExtendedTime = UserModel.extendTime(withViaPoints)(cellCatalogue.value)
      val modelEntities = UserModel.userCentric(withExtendedTime)(cellCatalogue.value)
      (modelEntities._1, modelEntities._2, modelEntities._3)
    })
  }
}

class DwellFunctions(self: RDD[Dwell]) {

  def byUserChronologically: RDD[(User, List[Dwell])] =
    self.map(d => (d.user, List(d))).reduceByKey(_ ++ _).map(userDwells =>
      (userDwells._1, userDwells._2.sortBy(_.startTime)))
}

class DwellStatistics(self: RDD[Dwell]) {

  lazy val daysWithDwells = dwellsPerDay.keys.distinct.collect
  lazy val daysWithDwellsAreas = areasPerDay.keys.distinct.collect
  lazy val daysWithDwellsDuration = durationsInMinutesPerDay.keys.distinct.collect

  def toDwellStats: RDD[UserModelStatsView] =
    totalNumberOfDwells ++ totalNumberOfUsersByDwells ++ meanDwellsByUser ++ stdDeviationDwellsByUser ++
      meanDwellsArea ++ stdDeviationDwellsArea ++ meanDwellsDuration ++ stdDeviationDwellsDuration

  private def totalNumberOfDwells = self.map(d =>
    ((d.formattedDay), 1)).reduceByKey(_ + _).map(s => UserModelStatsView(TotalNumberOfDwells, s._1, s._2))

  private def totalNumberOfUsersByDwells = self.map(d =>
    (d.formattedDay, Set[(String, Float)]((d.user.imsi, d.user.msisdn)))).reduceByKey((a, b) =>
      a ++ b).map(e => UserModelStatsView(TotalNumberOfUsersByDwells, e._1, e._2.size))

  private def dwellsPerDay = self.map(dwell =>
    ((dwell.formattedDay, dwell.user.imsi, dwell.user.msisdn), 1)).reduceByKey(_ + _).map(dwellsPerUserAndDay =>
      (dwellsPerUserAndDay._1._1, dwellsPerUserAndDay._2))

  private def statsDwellsByUser =
    self.sparkContext.parallelize(daysWithDwells.map(day => (day, dwellsPerDay.filter(e => e._1 == day).values.stats)))

  private def meanDwellsByUser = statsDwellsByUser.map(stat =>
    (UserModelStatsView(MeanDwellsByUser, stat._1, stat._2.mean)))

  private def stdDeviationDwellsByUser =
    statsDwellsByUser.map(stat => (UserModelStatsView(StdDeviationDwellsByUser, stat._1, stat._2.stdev)))

  private def areasPerDay = self.map(dwell =>
    ((dwell.formattedDay, GeomUtils.parseWkt(dwell.geomWkt, Coordinates.SaudiArabiaUtmSrid).getArea)))

  private def statsDwellsArea = self.sparkContext.parallelize(daysWithDwellsAreas.map(day =>
      (day, areasPerDay.filter(e => e._1 == day).values.stats)))

  private def meanDwellsArea = statsDwellsArea.map(stat => (UserModelStatsView(MeanDwellsArea, stat._1, stat._2.mean)))

  private def stdDeviationDwellsArea =
    statsDwellsArea.map(stat => (UserModelStatsView(StdDeviationDwellsArea, stat._1, stat._2.stdev)))

  private def durationsInMinutesPerDay= self.map(dwell => (dwell.formattedDay, dwell.durationInMinutes))

  private def statsDwellsDuration: RDD[(String, StatCounter)] =
    self.sparkContext.parallelize(daysWithDwellsDuration.map(day =>
      (day, durationsInMinutesPerDay.filter(e => e._1 == day).values.stats)))

  private def meanDwellsDuration =
    statsDwellsDuration.map(stat => (UserModelStatsView(MeanDwellsDuration, stat._1, stat._2.mean)))

  private def stdDeviationDwellsDuration =
    statsDwellsDuration.map(stat => (UserModelStatsView(StdDeviationDwellsDuration, stat._1, stat._2.stdev)))
}

trait UserModelDsl {

  implicit def userModelEventFunctions(userEventsWithMatchingCell: RDD[(User, List[Event])]): UserModelEventFunctions =
    new UserModelEventFunctions(userEventsWithMatchingCell)

  implicit def userModelSlotFunctions(userSlots: RDD[(User, List[SpatioTemporalSlot])]): UserModelSlotFunctions =
    new UserModelSlotFunctions(userSlots)

  implicit def dwellReader(csv: RDD[String]): DwellReader = new DwellReader(csv)

  implicit def journeyReader(csv: RDD[String]): JourneyReader = new JourneyReader(csv)

  implicit def journeyViaPointReader(csv: RDD[String]): JourneyViaPointReader = new JourneyViaPointReader(csv)

  implicit def dwellRowReader(self: RDD[Row]): DwellRowReader = new DwellRowReader(self)

  implicit def journeyRowReader(self: RDD[Row]): JourneyRowReader = new JourneyRowReader(self)

  implicit def journeyViaPointRowReader(self: RDD[Row]): JourneyViaPointRowReader = new JourneyViaPointRowReader(self)

  implicit def dwellWriter(dwells: RDD[Dwell]): DwellWriter = new DwellWriter(dwells)

  implicit def journeyWriter(journeys: RDD[Journey]): JourneyWriter = new JourneyWriter(journeys)

  implicit def journeyViaPointWriter(journeyViaPoints: RDD[JourneyViaPoint]): JourneyViaPointWriter =
    new JourneyViaPointWriter(journeyViaPoints)

  implicit def dwellFunctions(dwells: RDD[Dwell]): DwellFunctions = new DwellFunctions(dwells)

  implicit def dwellStatistics(dwells: RDD[Dwell]): DwellStatistics = new DwellStatistics(dwells)
}

object UserModelDsl extends UserModelDsl with JourneyDsl with EventDsl with CellDsl
