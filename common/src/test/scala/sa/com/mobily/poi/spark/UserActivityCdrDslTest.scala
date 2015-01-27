/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi.spark

import com.github.nscala_time.time.Imports._
import org.apache.spark.mllib.linalg.Vectors
import org.scalatest._

import sa.com.mobily.poi.UserActivityCdr
import sa.com.mobily.user.User
import sa.com.mobily.utils.{EdmCoreUtils, LocalSparkContext}

class UserActivityCdrDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import UserActivityCdrDsl._

  trait WithUserActivityCdrDslText {

    val userActivityCdr1 = "0500001413|20140824|2541|1|1,2"
    val userActivityCdr2 = "0500001413|20140824|2806|1|13,19,20"
    val userActivityCdr3 = "XXXXXX|20140824|4576|1|19"
    val userActivityCdr4 = "0500001413|20140825|6051|1|10,11"
    val userActivityCdr5 = "0500001413|20140825|5346|1|1,2,3,4,5,7,8,9"

    val userActivities = sc.parallelize(
      List(userActivityCdr1, userActivityCdr2, userActivityCdr3, userActivityCdr4, userActivityCdr5))
  }

  trait WithUserActivitiesCdr {

    val userActivityCdr1 = UserActivityCdr(
      User("", "", 1L),
      DateTimeFormat.forPattern("yyyymmdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia)
        .parseDateTime("20140824"),"2541", 1, Seq(1, 2))
    val userActivityCdr2 = UserActivityCdr(
      User("", "", 1L),
      DateTimeFormat.forPattern("yyyymmdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia)
        .parseDateTime("20140824"),"2555", 1, Seq(1, 2))
    val userActivityCdr3 = UserActivityCdr(
      User("", "", 2L),
      DateTimeFormat.forPattern("yyyymmdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia)
        .parseDateTime("20140824"),"2566", 1, Seq(1, 2))
    val userActivityCdr4 = UserActivityCdr(
      User("", "", 2L),
      DateTimeFormat.forPattern("yyyymmdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia)
        .parseDateTime("20140824"),"2566", 1, Seq(1, 2, 3))
    val userActivityCdr5 = UserActivityCdr(
      User("", "", 3L),
      DateTimeFormat.forPattern("yyyymmdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia)
        .parseDateTime("20140824"),"2577", 1, Seq(1, 2))

    val userActivitiesCdr =
      sc.parallelize(List(userActivityCdr1, userActivityCdr2, userActivityCdr3, userActivityCdr4, userActivityCdr5))
  }

  trait WithWeekUserActivityCdrs {

    val userActivityCdr1 =
      UserActivityCdr(
        User("", "", 1L),
        DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140818"),
        "2541",
        1,
        Seq(0, 1, 2))
    val userActivityCdr2 = userActivityCdr1.copy(
      timestamp =
        DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140819"),
      activityHours = Seq(1))
    val userActivityCdr3 =
      UserActivityCdr(
        User("", "", 2L),
        DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140825"),
        "2566",
        1,
        Seq(1, 2, 3))

    val userActivityCdrs = sc.parallelize(List(userActivityCdr1, userActivityCdr2, userActivityCdr3))
    val vectorResult = Vectors.sparse(UserActivityCdr.HoursInWeek, Seq((24, 1.0), (25, 1.0), (26, 1.0), (49, 1.0)))
  }

  trait WithWeekUserActivityCdrLittleActivity {

    val userActivityCdr1 = UserActivityCdr(
      User("", "", 1L),
      DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia)
        .parseDateTime("20140824"),"2541", 1, Seq(0, 1, 2, 4, 5, 6, 7))
    val userActivityCdr2 = userActivityCdr1.copy(
      timestamp =
        DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140818"),
      activityHours = Seq(0, 1, 2, 3, 4, 5, 6, 7))
    val userActivityCdr3 = userActivityCdr1.copy(
      timestamp =
        DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140819"),
      activityHours = Seq(1, 2, 3, 4, 5, 6, 7, 8, 23))
    val userActivityCdr4 = UserActivityCdr(
      User("", "", 2L),
      DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia)
        .parseDateTime("20140825"),"2566", 1, Seq(1, 2, 3))

    val userActivitiesCdrs =
      sc.parallelize(List(userActivityCdr1, userActivityCdr2, userActivityCdr3, userActivityCdr4))
  }

  trait WithSeveralWeeksUserActivityCdr {

    val userActivityCdrUser1Week1 = UserActivityCdr(
      User("", "", 1L),
      DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20150104"),
      "2541",
      1,
      Seq(0, 1, 2))
    val userActivityCdrUser1Week2 = userActivityCdrUser1Week1.copy(
      timestamp =
        DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20150111"),
      activityHours = Seq(0, 1))
    val userActivityCdrUser1Week3 = userActivityCdrUser1Week1.copy(
      timestamp =
        DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20150118"),
      activityHours = Seq(0))
    val userActivityCdrUser2Week1 = UserActivityCdr(
      User("", "", 2L),
      DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20150104"),
      "2566",
      1,
      Seq(0, 1, 2))
    val userActivitiesCdr = sc.parallelize(
      List(userActivityCdrUser1Week1, userActivityCdrUser1Week2, userActivityCdrUser1Week3, userActivityCdrUser2Week1))
    val expectedUser1AverageVector = Vectors.sparse(
      UserActivityCdr.HoursInWeek,
      Seq((0, 1.0), (1, 0.6666666666666666), (2, 0.3333333333333333)))
    val expectedUser2AverageVector = Vectors.sparse(UserActivityCdr.HoursInWeek, Seq((0, 1.0), (1, 1.0), (2, 1.0)))
  }

  "UserActivityCdrDsl" should "get correctly parsed phoneCalls" in new WithUserActivityCdrDslText {
    userActivities.toParsedUserActivityCdr.count() should be (5)
  }

  it should "get errors when parsing phoneCalls" in new WithUserActivityCdrDslText {
    userActivities.toUserActivityCdrErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed phoneCalls" in new WithUserActivityCdrDslText {
    userActivities.toUserActivityCdr.count should be (4)
  }

  it should "calculate the vector to home clustering correctly" in new WithWeekUserActivityCdrs {
    val homes = userActivityCdrs.toUserActivity.collect
    homes.length should be (2)
    homes.tail.head.activityVector should be (vectorResult)
  }
}
