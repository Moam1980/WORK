/*
 * TODO: License goes here!
 */

package sa.com.mobily.mobility.spark

import scala.language.implicitConversions

import com.github.nscala_time.time.Imports._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import sa.com.mobily.crm.{ProfilingCategory, SubscriberProfilingView}
import sa.com.mobily.mobility.{MobilityMatrixItem, MobilityMatrixItemParquet, MobilityMatrixView}
import sa.com.mobily.parsing.spark.{SparkWriter, SparkParser}
import sa.com.mobily.utils.EdmCoreUtils

class MobilityMatrixItemRowReader(self: RDD[Row]) {

  def toMobilityMatrixItem: RDD[MobilityMatrixItem] =
    SparkParser.fromRow[MobilityMatrixItemParquet](self).map(itemParquet => MobilityMatrixItem(itemParquet))
}

class MobilityMatrixItemWriter(self: RDD[MobilityMatrixItem]) {

  def saveAsParquetFile(path: String): Unit =
    SparkWriter.saveAsParquetFile[MobilityMatrixItemParquet](self.map(i => MobilityMatrixItemParquet(i)), path)
}

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

  def forProfilingCategory(
      targetCategory: ProfilingCategory,
      subscribersProfiling: RDD[SubscriberProfilingView]): RDD[MobilityMatrixItem] = {
    val subsWithinCategory =
      subscribersProfiling.filter(_.category == targetCategory).map(s => (s.imsi, None)).collect.toMap
    self.filter(item => subsWithinCategory.contains(item.user.imsi))
  }
}

trait MobilityMatrixDsl {

  implicit def mobilityMatrixItemRowReader(self: RDD[Row]): MobilityMatrixItemRowReader =
    new MobilityMatrixItemRowReader(self)

  implicit def mobilityMatrixItemWriter(mobilityMatrixItems: RDD[MobilityMatrixItem]): MobilityMatrixItemWriter =
    new MobilityMatrixItemWriter(mobilityMatrixItems)

  implicit def mobilityMatrixFunctions(mobilityMatrix: RDD[MobilityMatrixItem]): MobilityMatrixFunctions =
    new MobilityMatrixFunctions(mobilityMatrix)
}

object MobilityMatrixDsl extends MobilityMatrixDsl
