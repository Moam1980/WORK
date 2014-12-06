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
import sa.com.mobily.usercentric.{UserModel, SpatioTemporalSlot}

class UserModelEventFunctions(userEventsWithMatchingCell: RDD[(Long, List[Event])]) {

  def aggSameCell(implicit cellCatalogue: Broadcast[Map[(Int, Int), Cell]]): RDD[(Long, List[SpatioTemporalSlot])] =
    userEventsWithMatchingCell.mapValues(events => UserModel.aggSameCell(events)(cellCatalogue.value))
}

class UserModelSlotFunctions(userSlots: RDD[(Long, List[SpatioTemporalSlot])]) {

  def combine: RDD[(Long, List[SpatioTemporalSlot])] = userSlots.mapValues(slots => {
    val slotsWithScores = UserModel.computeScores(slots)
    UserModel.aggregateCompatible(slotsWithScores)
  })
}

trait UserModelDsl {

  implicit def userModelEventFunctions(userEventsWithMatchingCell: RDD[(Long, List[Event])]): UserModelEventFunctions =
    new UserModelEventFunctions(userEventsWithMatchingCell)

  implicit def userModelSlotFunctions(userSlots: RDD[(Long, List[SpatioTemporalSlot])]): UserModelSlotFunctions =
    new UserModelSlotFunctions(userSlots)
}

object UserModelDsl extends UserModelDsl with EventDsl with CellDsl
