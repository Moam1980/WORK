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

  def perUserAndSiteId: RDD[((Long, Int, Int, String, Long), Vector)] = {
    val byUserWeekYearRegion: RDD[((Long, Int, Int, String, Long), Iterable[(Int, Seq[Int])])] = self.map(phoneCall => {
      val callDate = new DateTime(phoneCall.timestamp, EdmCoreUtils.TimeZoneSaudiArabia)
      ((phoneCall.msisdn, callDate.year.get, callDate.weekOfWeekyear.get, phoneCall.siteId, phoneCall.regionId),
        (callDate.dayOfWeek.get, phoneCall.callHours))
    }).groupByKey
    byUserWeekYearRegion.mapValues(callsByWeek => {
      val sortedCallsByWeek = callsByWeek.toSeq.sortBy(callsByDay => callsByDay._1)
      val daysWithCalls = sortedCallsByWeek.map(dayCalls => dayCalls._1).toIndexedSeq
      val activityHour = (1 to 7).flatMap(day =>
        if (daysWithCalls.contains(day)) {
          val hoursWithCalls = sortedCallsByWeek(daysWithCalls.indexOf(day))._2
          (0 to 23).map(hour => if (hoursWithCalls.contains(hour)) 1D else 0D)
        } else (0 to 23).map(_ => 0D))
      Vectors.dense(activityHour.toArray)
    })
  }
}

trait UserPhoneCallsDsl {

  implicit def phoneCallsReader(csv: RDD[String]): UserPhoneCallsReader = new UserPhoneCallsReader(csv)

  implicit def phoneCallsFunctions(phoneCalls: RDD[UserPhoneCalls]): UserPhoneCallsFunctions =
    new UserPhoneCallsFunctions(phoneCalls)
}

object UserPhoneCallsDsl extends UserPhoneCallsDsl with ParsedItemsDsl
