/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import sa.com.mobily.event.spark.EventDsl
import sa.com.mobily.parsing.spark.{SparkParser, SparkWriter}
import sa.com.mobily.poi.{UserActivity, UserActivityParquet}

class UserActivityFunctions(self: RDD[UserActivity]) {

  import UserActivity._

  def byYearWeek: RDD[UserActivity] = self.keyBy(_.keyByWeek).reduceByKey(_.combineByWeekYear(_)).values

  def removeLittleActivity(minimumActivityRatio: Double = DefaultMinActivityRatio): RDD[UserActivity] = {
    val minNumberOfHours = HoursPerWeek * minimumActivityRatio
    self.filter(_.weekHoursWithActivity.size > minNumberOfHours)
  }

  def aggregateActivity: RDD[UserActivity] = self.keyBy(_.key).reduceByKey(_.aggregate(_)).values
}

class UserActivityWriter(self: RDD[UserActivity]) {

  def saveAsParquetFile(path: String): Unit = {
    val rdd = self.map(userActivity => UserActivityParquet(userActivity))
    SparkWriter.saveAsParquetFile[UserActivityParquet](rdd, path)
  }
}

class UserActivityReader(self: RDD[Row]) {

  def toUserActivity: RDD[UserActivity] = toUserActivityParquet.map(_.toUserActivity)

  def toUserActivityParquet: RDD[UserActivityParquet] = SparkParser.fromRow[UserActivityParquet](self)
}

trait UserActivityDsl {

  implicit def userActivityFunctions(userActivities: RDD[UserActivity]): UserActivityFunctions =
    new UserActivityFunctions(userActivities)

  implicit def userActivityWriter(self: RDD[UserActivity]): UserActivityWriter = new UserActivityWriter(self)

  implicit def userActivityReader(self: RDD[Row]): UserActivityReader = new UserActivityReader(self)
}

object UserActivityDsl extends UserActivityDsl with EventDsl
