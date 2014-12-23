/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi.spark

import sa.com.mobily.user.User

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser}
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.poi.{UserActivity, UserActivityCdr}
import sa.com.mobily.utils.EdmCoreUtils

class UserActivityCdrReader(self: RDD[String]) {

  import sa.com.mobily.parsing.spark.ParsedItemsDsl._

  def toParsedUserActivityCdr: RDD[ParsedItem[UserActivityCdr]] =
    SparkParser.fromCsv[UserActivityCdr](self)(UserActivityCdr.fromCsv)

  def toPhoneCallsErrors: RDD[ParsingError] = toParsedUserActivityCdr.errors

  def toUserActivityCdr: RDD[UserActivityCdr] = toParsedUserActivityCdr.values
}

class UserActivityCdrFunctions(self: RDD[UserActivityCdr]) {

  import UserActivityCdr._

  def perUserAndSiteId: RDD[UserActivity] = {
    val byUserWeekYearRegion: RDD[((Long, String, Short), Iterable[(Int, Seq[Int])])] = self.map(phoneCall => {
      val callDate = new DateTime(phoneCall.timestamp, EdmCoreUtils.TimeZoneSaudiArabia)
      ((phoneCall.user.msisdn, phoneCall.siteId, phoneCall.regionId),
        (EdmCoreUtils.saudiDayOfWeek(callDate.dayOfWeek.get), phoneCall.activityHours))
    }).groupByKey
    byUserWeekYearRegion.mapValues(callsByWeek => {
      val activityHoursByWeek = callsByWeek.flatMap((callsByDay: (Int, Seq[Int])) => {
        callsByDay._2.map(hour => (weekHour(callsByDay._1, hour), 1D))
      }).toSeq
      Vectors.sparse(HoursInWeek, activityHoursByWeek)
    }).map(activity => UserActivity(User("", "", activity._1._1), activity._1._2, activity._1._3, activity._2))
  }

  def perUserAndSiteIdFilteringLittleActivity(
    minimumActivityRatio: Double = DefaultMinActivityRatio): RDD[UserActivity] = {
    val minNumberOfHours = HoursInWeek * minimumActivityRatio
    perUserAndSiteId.filter(element => element.activityVector.toArray.count(element => element == 1) > minNumberOfHours)
  }
}

trait UserActivityCdrDsl {

  implicit def phoneCallsReader(csv: RDD[String]): UserActivityCdrReader = new UserActivityCdrReader(csv)

  implicit def phoneCallsFunctions(phoneCalls: RDD[UserActivityCdr]): UserActivityCdrFunctions =
    new UserActivityCdrFunctions(phoneCalls)
}

object UserActivityCdrDsl extends UserActivityCdrDsl with ParsedItemsDsl
