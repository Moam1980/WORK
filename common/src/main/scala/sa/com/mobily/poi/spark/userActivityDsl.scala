/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import sa.com.mobily.event.spark.EventDsl
import sa.com.mobily.poi.UserActivity

class UserActivityFunctions(self: RDD[UserActivity]) {

  import UserActivity._

  def byYearWeek: RDD[UserActivity] = self.keyBy(_.keyByWeek).reduceByKey(_.combineByWeekYear(_)).values

  def removeLittleActivity(minimumActivityRatio: Double = DefaultMinActivityRatio): RDD[UserActivity] = {
    val minNumberOfHours = HoursPerWeek * minimumActivityRatio
    self.filter(_.weekHoursWithActivity.size > minNumberOfHours)
  }

  def aggregateActivity: RDD[UserActivity] = self.keyBy(_.key).reduceByKey(_.aggregate(_)).values
}

trait UserActivityDsl {

  implicit def userActivityFunctions(userActivities: RDD[UserActivity]): UserActivityFunctions =
    new UserActivityFunctions(userActivities)
}

object UserActivityDsl extends UserActivityDsl with EventDsl
