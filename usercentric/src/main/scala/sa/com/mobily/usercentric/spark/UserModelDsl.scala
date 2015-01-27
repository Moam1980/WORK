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
import sa.com.mobily.user.User
import sa.com.mobily.usercentric._

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
}

object UserModelDsl extends UserModelDsl with JourneyDsl with EventDsl with CellDsl
