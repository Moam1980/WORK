/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi.spark

import com.github.nscala_time.time.Imports._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser}
import sa.com.mobily.poi.UserPhoneCalls
import sa.com.mobily.utils.EdmCoreUtils

import scala.language.implicitConversions

class UserPhoneCallsReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedPhoneCalls: RDD[ParsedItem[UserPhoneCalls]] =
    SparkParser.fromCsv[UserPhoneCalls](self)(UserPhoneCalls.fromCsv)

  def toPhoneCallsErrors: RDD[ParsingError] = toParsedPhoneCalls.errors

  def toPhoneCalls: RDD[UserPhoneCalls] = toParsedPhoneCalls.values
}

class UserPhoneCallsFunctions(self: RDD[UserPhoneCalls]) {

  import UserPhoneCalls._

  def perUserAndSiteId: RDD[((Long, Int, Int, String, Long), Vector)] = {
    val byUserWeekYearRegion: RDD[((Long, Int, Int, String, Long), Iterable[(Int, Seq[Int])])] = self.map(phoneCall => {
      val callDate = new DateTime(phoneCall.timestamp, EdmCoreUtils.TimeZoneSaudiArabia)
      ((phoneCall.msisdn, callDate.year.get, callDate.weekOfWeekyear.get, phoneCall.siteId, phoneCall.regionId),
        (callDate.dayOfWeek.get, phoneCall.callHours))
    }).groupByKey
    byUserWeekYearRegion.mapValues(callsByWeek => {
      val activityHoursByWeek = callsByWeek.flatMap((callsByDay: (Int, Seq[Int])) => {
        callsByDay._2.map(hour => (weekHour(callsByDay._1, hour), 1D))
      }).toSeq
      Vectors.sparse(HoursInWeek, activityHoursByWeek)
    })
  }

  def perUserAndSiteIdFilteringLittleActivity(
    minimumActivityRatio: Double = DefaultMinActivityRatio): RDD[((Long, Int, Int, String, Long), Vector)] = {
    val activityPercentageThreshold = HoursInWeek * minimumActivityRatio
    perUserAndSiteId.filter(element => element._2.toArray.count(element => element == 1) > activityPercentageThreshold)
  }
}

trait UserPhoneCallsDsl {

  implicit def phoneCallsReader(csv: RDD[String]): UserPhoneCallsReader = new UserPhoneCallsReader(csv)

  implicit def phoneCallsFunctions(phoneCalls: RDD[UserPhoneCalls]): UserPhoneCallsFunctions =
    new UserPhoneCallsFunctions(phoneCalls)
}

object UserPhoneCallsDsl extends UserPhoneCallsDsl with ParsedItemsDsl
