/*
 * TODO: License goes here!
 */

package sa.com.mobily.visit

import com.github.nscala_time.time.Imports._
import org.scalatest._

import sa.com.mobily.user.User
import sa.com.mobily.usercentric.Dwell
import sa.com.mobily.utils.EdmCoreUtils

class UserVisitMetricsTest extends FlatSpec with ShouldMatchers {

  trait WithUserVisitMetrics {

    val tolerance = 1e-6

    val user1 = User(imei = "", imsi = "420031", msisdn = 9661)
    val int1 = new Interval(
      new DateTime(0, EdmCoreUtils.TimeZoneSaudiArabia),
      new DateTime(1800000, EdmCoreUtils.TimeZoneSaudiArabia))

    val userVisitMetrics =
      UserVisitMetrics(
        user = user1,
        location = "loc1",
        interval = int1,
        frequency = 3,
        durationInBetweenVisits = List(new Duration(900000, 1800000), new Duration(2100000, 2400000)),
        visitDurations = List(new Duration(0, 900000), new Duration(1800000, 2100000), new Duration(2400000, 3300000)),
        geomPrecisions = List(1500000, 500000, 2500000))

    val dwell1 =
      Dwell(
        user = user1,
        startTime = 0,
        endTime = 1800000,
        geomWkt = "POLYGON ((0 0, 0 1000, 1000 1000, 1000 0, 0 0))",
        cells = Seq((2, 4), (2, 6)),
        firstEventBeginTime = 5000,
        lastEventEndTime = 1700000,
        numEvents = 2)
    val dwell2 = dwell1.copy(startTime = 2100000, endTime = 2700000,
      geomWkt = "POLYGON ((0 0, 0 2500, 2500 2500, 2500 0, 0 0))")
    val dwell3 = dwell1.copy(startTime = 3300000, endTime = 4200000)
    val dwellsChronologicallyOrdered = List(dwell1, dwell2, dwell3)
    val userVisitMetricsFromDwells =
      UserVisitMetrics(
        user = user1,
        location = "loc1",
        interval = int1,
        frequency = 3,
        durationInBetweenVisits = List(new Duration(1800000, 2100000), new Duration(2700000, 3300000)),
        visitDurations = List(new Duration(0, 1800000), new Duration(2100000, 2700000), new Duration(3300000, 4200000)),
        geomPrecisions = List(1000000, 6250000, 1000000))
    val userVisitMetricsFromDwell1 =
      UserVisitMetrics(
        user = user1,
        location = "loc1",
        interval = int1,
        frequency = 1,
        durationInBetweenVisits = List(),
        visitDurations = List(new Duration(0, 1800000)),
        geomPrecisions = List(1000000))
  }

  "UserVisitMetrics" should "compute average recency in minutes" in new WithUserVisitMetrics {
    userVisitMetrics.avgRecencyInMinutes should be (10)
  }

  it should "compute average recency in minutes when there is only one visit" in new WithUserVisitMetrics {
    userVisitMetrics.copy(frequency = 1, durationInBetweenVisits = List()).avgRecencyInMinutes should be (0)
  }

  it should "compute average visit duration in minutes" in new WithUserVisitMetrics {
    userVisitMetrics.avgVisitDurationInMinutes should be (11.666666 +- tolerance)
  }

  it should "compute average geometrical precision" in new WithUserVisitMetrics {
    userVisitMetrics.avgGeomPrecision should be (1500000)
  }

  it should "return the key for the entity" in new WithUserVisitMetrics {
    userVisitMetrics.key should be (("loc1", int1))
  }

  it should "return the fields" in new WithUserVisitMetrics {
    userVisitMetrics.fields should
      be (Array("loc1", "1970-01-01 03:00:00", "1970-01-01 03:30:00", "", "420031", "9661", "3", "10", "12", "1500000"))
  }

  it should "return the header" in {
    UserVisitMetrics.Header should
      be (Array("location", "startTime", "endTime", "imei", "imsi", "msisdn", "frequency", "avgRecencyInMinutes",
        "avgVisitDurationInMinutes", "avgGeomPrecision"))
  }

  it should "have the same number of fields in header and fields returned" in new WithUserVisitMetrics {
    userVisitMetrics.fields.length should be (UserVisitMetrics.Header.length)
  }

  it should "build from location, interval and dwells chronologically ordered" in new WithUserVisitMetrics {
    UserVisitMetrics("loc1", int1, dwellsChronologicallyOrdered) should be (userVisitMetricsFromDwells)
  }

  it should "build from location, interval and a single dwell" in new WithUserVisitMetrics {
    UserVisitMetrics("loc1", int1, List(dwell1)) should be (userVisitMetricsFromDwell1)
  }

  it should "not build when number of dwells is less than one" in new WithUserVisitMetrics {
    an[Exception] should be thrownBy UserVisitMetrics("loc1", int1, List())
  }
}
