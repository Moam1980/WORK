/*
 * TODO: License goes here!
 */

package sa.com.mobily.visit

import com.github.nscala_time.time.Imports._
import org.scalatest._

import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils

class FootfallTest extends FlatSpec with ShouldMatchers {

  trait WithFootfalls {

    val user1 = User(imei = "", imsi = "420031", msisdn = 9661)
    val user2 = User(imei = "", imsi = "420032", msisdn = 9662)
    val user3 = User(imei = "", imsi = "420033", msisdn = 9663)
    val int1 = new Interval(
      new DateTime(0, EdmCoreUtils.TimeZoneSaudiArabia),
      new DateTime(1800000, EdmCoreUtils.TimeZoneSaudiArabia))
    val footfallWithRecency =
      Footfall(
        users = Set(user1, user2, user3),
        location = "loc1",
        interval = int1,
        frequency = 7,
        recencyCounter = 2,
        avgRecencyInMinutes = 75,
        avgVisitDurationInMinutes = 25,
        avgGeomPrecision = 100000)

    val userVisitMetrics =
      UserVisitMetrics(
        user = user1,
        location = "loc1",
        interval = int1,
        frequency = 2,
        durationInBetweenVisits = List(new Duration(905000, 1805000)),
        visitDurations = List(new Duration(0, 900000), new Duration(1805000, 2105000)),
        geomPrecisions = List(1500000, 500000))
    val userVisitMetricsFootfall =
      Footfall(
        users = Set(user1),
        location = "loc1",
        interval = int1,
        frequency = 2,
        recencyCounter = 1,
        avgRecencyInMinutes = 15,
        avgVisitDurationInMinutes = 10,
        avgGeomPrecision = 1000000)

    val ff1WithoutRecency =
      Footfall(
        users = Set(user1, user2),
        location = "loc1",
        interval = int1,
        frequency = 1,
        recencyCounter = 0,
        avgRecencyInMinutes = 0,
        avgVisitDurationInMinutes = 35,
        avgGeomPrecision = 200000)
    val ff2WithoutRecency = ff1WithoutRecency.copy(users = Set(user2, user3), avgVisitDurationInMinutes = 45)
    val aggFootfallWithoutRecencies =
      Footfall(
        users = Set(user1, user2, user3),
        location = "loc1",
        interval = int1,
        frequency = 2,
        recencyCounter = 0,
        avgRecencyInMinutes = 0,
        avgVisitDurationInMinutes = 40,
        avgGeomPrecision = 200000)

    val aggFootfallff1WithoutRecency =
      Footfall(
        users = Set(user1, user2, user3),
        location = "loc1",
        interval = int1,
        frequency = 8,
        recencyCounter = 2,
        avgRecencyInMinutes = 75,
        avgVisitDurationInMinutes = 26.25,
        avgGeomPrecision = 112500)

    val aggFootfallBothWithRecencies =
      Footfall(
        users = Set(user1, user2, user3),
        location = "loc1",
        interval = int1,
        frequency = 9,
        recencyCounter = 3,
        avgRecencyInMinutes = 55,
        avgVisitDurationInMinutes = 21.666666666666668,
        avgGeomPrecision = 300000)
  }

  "Footfall" should "compute footfall" in new WithFootfalls {
    footfallWithRecency.numDistinctUsers should be (3)
  }

  it should "return header" in new WithFootfalls {
    Footfall.Header should be (Array("location", "startTime", "endTime", "footfall", "frequency", "avgRecencyInMinutes",
      "avgVisitDurationInMinutes", "avgGeomPrecision"))
  }

  it should "return fields" in new WithFootfalls {
    footfallWithRecency.fields should
      be (Array("loc1", "1970-01-01 03:00:00", "1970-01-01 03:30:00", "3", "7", "75", "25", "100000"))
  }

  it should "have the same number of fields in header and fields returned" in new WithFootfalls {
    footfallWithRecency.fields.length should be (Footfall.Header.length)
  }

  it should "build from UserVisitMetrics" in new WithFootfalls {
    Footfall(userVisitMetrics) should be (userVisitMetricsFootfall)
  }

  it should "aggregate footfalls when recency counters are zero" in new WithFootfalls {
    Footfall.aggregate(ff1WithoutRecency, ff2WithoutRecency) should be (aggFootfallWithoutRecencies)
  }

  it should "aggregate footfalls when recency counter for one of them is zero" in new WithFootfalls {
    Footfall.aggregate(footfallWithRecency, ff1WithoutRecency) should be (aggFootfallff1WithoutRecency)
  }

  it should "aggregate footfalls when recency counters are not equal to zero" in new WithFootfalls {
    Footfall.aggregate(footfallWithRecency, userVisitMetricsFootfall) should be (aggFootfallBothWithRecencies)
  }
}
