/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser}
import sa.com.mobily.poi.{UserActivity, UserActivityCdr}
import sa.com.mobily.utils.EdmCoreUtils

class UserActivityCdrReader(self: RDD[String]) {

  import sa.com.mobily.parsing.spark.ParsedItemsDsl._

  def toParsedUserActivityCdr: RDD[ParsedItem[UserActivityCdr]] =
    SparkParser.fromCsv[UserActivityCdr](self)(UserActivityCdr.fromCsv)

  def toUserActivityCdrErrors: RDD[ParsingError] = toParsedUserActivityCdr.errors

  def toUserActivityCdr: RDD[UserActivityCdr] = toParsedUserActivityCdr.values
}

class UserActivityCdrFunctions(self: RDD[UserActivityCdr]) {

  import UserActivityCdr._

  def toUserActivity: RDD[UserActivity] = {
    val byUserWeekYearRegion = self.map(userActivity => {
      val activityDate = new DateTime(userActivity.timestamp, EdmCoreUtils.TimeZoneSaudiArabia)
      ((userActivity.user, userActivity.siteId,
        userActivity.regionId, activityDate.year.get.toShort, EdmCoreUtils.saudiWeekOfYear(activityDate).toShort),
        (EdmCoreUtils.saudiDayOfWeek(activityDate.dayOfWeek.get), userActivity.activityHours))
    }).groupByKey
    byUserWeekYearRegion.map(activityByWeek => {
      val userActivity = activityByWeek._2
      val key = activityByWeek._1
      val activityHoursByWeek = userActivity.flatMap(activityByDay => {
        activityByDay._2.map(hour => (weekHour(activityByDay._1, hour), 1D))
      }).toMap
      UserActivity(key._1, key._2, key._3, activityHoursByWeek, Set((key._4, key._5)))
    })
  }
}

trait UserActivityCdrDsl {

  implicit def userActivityCdrReader(csv: RDD[String]): UserActivityCdrReader = new UserActivityCdrReader(csv)

  implicit def userActivityCdrFunctions(userActivities: RDD[UserActivityCdr]): UserActivityCdrFunctions =
    new UserActivityCdrFunctions(userActivities)
}

object UserActivityCdrDsl extends UserActivityCdrDsl with ParsedItemsDsl
