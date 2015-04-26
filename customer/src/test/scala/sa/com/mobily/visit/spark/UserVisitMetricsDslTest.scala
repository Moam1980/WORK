/*
 * TODO: License goes here!
 */

package sa.com.mobily.visit.spark

import com.github.nscala_time.time.Imports._
import org.scalatest._
import sa.com.mobily.user.User

import sa.com.mobily.utils.{EdmCoreUtils, LocalSparkContext}
import sa.com.mobily.visit.{Footfall, UserVisitMetrics}

class UserVisitMetricsDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import UserVisitMetricsDsl._

  trait WithUserVisitMetrics {

    val user1 = User(imei = "", imsi = "420031", msisdn = 9661)
    val user2 = User(imei = "", imsi = "420032", msisdn = 9662)
    val user3 = User(imei = "", imsi = "420033", msisdn = 9663)
    val int1 = new Interval(
      new DateTime(0, EdmCoreUtils.TimeZoneSaudiArabia),
      new DateTime(1800000, EdmCoreUtils.TimeZoneSaudiArabia))

    val userVisitMetrics1 =
      UserVisitMetrics(
        user = user1,
        location = "loc1",
        interval = int1,
        frequency = 3,
        durationInBetweenVisits = List(new Duration(900000, 1800000), new Duration(2100000, 2400000)),
        visitDurations = List(new Duration(0, 900000), new Duration(1800000, 2100000), new Duration(2400000, 3300000)),
        geomPrecisions = List(1500000, 500000, 2500000))
    val userVisitMetrics2 =
      UserVisitMetrics(
        user = user2,
        location = "loc1",
        interval = int1,
        frequency = 2,
        durationInBetweenVisits = List(new Duration(1800000, 5400000)),
        visitDurations = List(new Duration(0, 1800000), new Duration(5400000, 9000000)),
        geomPrecisions = List(900000, 700000))
    val userVisitMetrics3 =
      UserVisitMetrics(
        user = user3,
        location = "loc1",
        interval = int1,
        frequency = 1,
        durationInBetweenVisits = List(),
        visitDurations = List(new Duration(0, 1800000)),
        geomPrecisions = List(800000))
    val userVisitMetrics4 =
      UserVisitMetrics(
        user = user1,
        location = "loc2",
        interval = int1,
        frequency = 2,
        durationInBetweenVisits = List(new Duration(1800000, 2700000)),
        visitDurations = List(new Duration(900000, 1800000), new Duration(2700000, 5400000)),
        geomPrecisions = List(700000, 600000))
    val userVisitMetrics =
      sc.parallelize(Array(userVisitMetrics1, userVisitMetrics2, userVisitMetrics3, userVisitMetrics4))

    val footfallLoc1Int1 =
      Footfall(
        users = Set(user1, user2, user3),
        location = "loc1",
        interval = int1,
        frequency = 6,
        recencyCounter = 3,
        avgRecencyInMinutes = 26.666666666666668,
        avgVisitDurationInMinutes = 25.833333333333332,
        avgGeomPrecision = 1150000)
    val footfallLoc2Int1 =
      Footfall(
        users = Set(user1),
        location = "loc2",
        interval = int1,
        frequency = 2,
        recencyCounter = 1,
        avgRecencyInMinutes = 15,
        avgVisitDurationInMinutes = 30,
        avgGeomPrecision = 650000)
    val footfalls = List(footfallLoc1Int1, footfallLoc2Int1)
  }

  "UserVisitMetricsDsl" should "compute footfall from UserVisitMetrics" in new WithUserVisitMetrics {
    userVisitMetrics.footfall.collect should contain theSameElementsAs (footfalls)
  }
}
