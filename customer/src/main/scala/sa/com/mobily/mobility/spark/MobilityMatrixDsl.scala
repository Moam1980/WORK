/*
 * TODO: License goes here!
 */

package sa.com.mobily.mobility.spark

import scala.language.implicitConversions

import com.github.nscala_time.time.Imports._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import sa.com.mobily.mobility.{MobilityMatrixItem, MobilityMatrixView}
import sa.com.mobily.utils.EdmCoreUtils

class MobilityMatrixFunctions(self: RDD[MobilityMatrixItem]) {

  def perDayGroups(
      timeBin: (DateTime) => String,
      numDaysWithinOneWeek: (DateTime, DateTime) => Int,
      minUsersPerJourney: Int = MobilityMatrixView.DefaultMinUsersPerJourney,
      keepJourneysBetweenSameLocations: Boolean = false): RDD[MobilityMatrixView] = {
    val viewItems =
      self.map(item => MobilityMatrixView(item = item, timeBin = timeBin, numDaysWithinOneWeek = numDaysWithinOneWeek))
    val aggViewItems = viewItems.keyBy(_.key).reduceByKey(MobilityMatrixView.aggregate).values
    val withProperLocations =
      if (keepJourneysBetweenSameLocations) aggViewItems
      else aggViewItems.filter(viewItem => viewItem.startLocation != viewItem.endLocation)
    withProperLocations.filter(_.avgWeight >= minUsersPerJourney)
  }

  def perDayOfWeek(
      minUsersPerJourney: Int = MobilityMatrixView.DefaultMinUsersPerJourney,
      keepJourneysBetweenSameLocations: Boolean = false): RDD[MobilityMatrixView] =
    perDayGroups(
      timeBin = (date) => MobilityMatrixView.WeekDayFmt.print(date),
      numDaysWithinOneWeek = (start, end) => 1,
      minUsersPerJourney = minUsersPerJourney,
      keepJourneysBetweenSameLocations = keepJourneysBetweenSameLocations)

  def perDay(
      minUsersPerJourney: Int = MobilityMatrixView.DefaultMinUsersPerJourney,
      keepJourneysBetweenSameLocations: Boolean = false): RDD[MobilityMatrixView] =
    perDayGroups(
      timeBin = (date) => MobilityMatrixView.TimeFmt.print(date),
      numDaysWithinOneWeek = (start, end) => EdmCoreUtils.DaysInWeek,
      minUsersPerJourney = minUsersPerJourney,
      keepJourneysBetweenSameLocations = keepJourneysBetweenSameLocations)
}

trait MobilityMatrixDsl {

  implicit def mobilityMatrixFunctions(mobilityMatrix: RDD[MobilityMatrixItem]): MobilityMatrixFunctions =
    new MobilityMatrixFunctions(mobilityMatrix)
}

object MobilityMatrixDsl extends MobilityMatrixDsl
