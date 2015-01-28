/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import sa.com.mobily.cell.Cell
import sa.com.mobily.cell.spark.CellDsl
import sa.com.mobily.event.Event
import sa.com.mobily.event.spark.EventDsl
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser}
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
      val modelEntities = UserModel.userCentric(withViaPoints)(cellCatalogue.value)
      (modelEntities._1, modelEntities._2, modelEntities._3)
    })
  }
}

trait UserModelDsl {

  implicit def userModelEventFunctions(userEventsWithMatchingCell: RDD[(User, List[Event])]): UserModelEventFunctions =
    new UserModelEventFunctions(userEventsWithMatchingCell)

  implicit def userModelSlotFunctions(userSlots: RDD[(User, List[SpatioTemporalSlot])]): UserModelSlotFunctions =
    new UserModelSlotFunctions(userSlots)

  implicit def dwellReader(csv: RDD[String]): DwellReader = new DwellReader(csv)

  implicit def journeyReader(csv: RDD[String]): JourneyReader = new JourneyReader(csv)

  implicit def journeyViaPointReader(csv: RDD[String]): JourneyViaPointReader = new JourneyViaPointReader(csv)
}

object UserModelDsl extends UserModelDsl with JourneyDsl with EventDsl with CellDsl
