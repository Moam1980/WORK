/*
 * TODO: License goes here!
 */

package sa.com.mobily.mobility

import org.apache.spark.sql.catalyst.expressions.Row

import sa.com.mobily.parsing.RowParser
import sa.com.mobily.user.User

case class MobilityMatrixItemParquet(
    timeZone: String,
    startIntervalStart: Long,
    startIntervalEnd: Long,
    endIntervalStart: Long,
    endIntervalEnd: Long,
    startLocation: String,
    endLocation: String,
    journeyDurationInMillis: Long,
    numWeeks: Int,
    user: User,
    origWeight: Double,
    destWeight: Double)

object MobilityMatrixItemParquet {

  def apply(mobilityMatrixItem: MobilityMatrixItem): MobilityMatrixItemParquet =
    MobilityMatrixItemParquet(
      timeZone = mobilityMatrixItem.startInterval.getStart.getZone.getID,
      startIntervalStart = mobilityMatrixItem.startInterval.getStartMillis,
      startIntervalEnd = mobilityMatrixItem.startInterval.getEndMillis,
      endIntervalStart = mobilityMatrixItem.endInterval.getStartMillis,
      endIntervalEnd = mobilityMatrixItem.endInterval.getEndMillis,
      startLocation = mobilityMatrixItem.startLocation,
      endLocation = mobilityMatrixItem.endLocation,
      journeyDurationInMillis = mobilityMatrixItem.journeyDuration.getMillis,
      numWeeks = mobilityMatrixItem.numWeeks,
      user = mobilityMatrixItem.user,
      origWeight = mobilityMatrixItem.origWeight,
      destWeight = mobilityMatrixItem.destWeight)

  implicit val fromRow = new RowParser[MobilityMatrixItemParquet] {

    override def fromRow(row: Row): MobilityMatrixItemParquet = {
      val Row(
        timeZone,
        startIntervalStart,
        startIntervalEnd,
        endIntervalStart,
        endIntervalEnd,
        startLocation,
        endLocation,
        journeyDurationInMillis,
        numWeeks,
        userRow,
        origWeight,
        destWeight) = row

      MobilityMatrixItemParquet(
        timeZone = timeZone.asInstanceOf[String],
        startIntervalStart = startIntervalStart.asInstanceOf[Long],
        startIntervalEnd = startIntervalEnd.asInstanceOf[Long],
        endIntervalStart = endIntervalStart.asInstanceOf[Long],
        endIntervalEnd = endIntervalEnd.asInstanceOf[Long],
        startLocation = startLocation.asInstanceOf[String],
        endLocation = endLocation.asInstanceOf[String],
        journeyDurationInMillis = journeyDurationInMillis.asInstanceOf[Long],
        numWeeks = numWeeks.asInstanceOf[Int],
        user = User.fromRow.fromRow(userRow.asInstanceOf[Row]),
        origWeight = origWeight.asInstanceOf[Double],
        destWeight = destWeight.asInstanceOf[Double])
    }
  }
}
