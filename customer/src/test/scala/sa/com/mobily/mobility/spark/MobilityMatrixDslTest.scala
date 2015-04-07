/*
 * TODO: License goes here!
 */

package sa.com.mobily.mobility.spark

import scala.reflect.io.File

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest._

import sa.com.mobily.crm.{ProfilingCategory, SubscriberProfilingView}
import sa.com.mobily.mobility.{MobilityMatrixItem, MobilityMatrixView}
import sa.com.mobily.user.User
import sa.com.mobily.utils.{EdmCoreUtils, LocalSparkSqlContext}

class MobilityMatrixDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import MobilityMatrixDsl._

  trait WithIntervals {

    val startDate = EdmCoreUtils.Fmt.parseDateTime("2014/11/02 00:00:00")
    val splitInFreq1 = startDate.plusHours(6)
    val splitInFreq2 = splitInFreq1.plusHours(4)
    val splitInFreq3 = splitInFreq2.plusHours(6)
    val splitInFreq4 = splitInFreq3.plusHours(3)
    val splitInFreq5 = splitInFreq4.plusHours(5)
    val splitIntervals1 = EdmCoreUtils.intervals(startDate, splitInFreq1, 60)
    val splitIntervals2 = EdmCoreUtils.intervals(splitInFreq1, splitInFreq2, 15)
    val splitIntervals3 = EdmCoreUtils.intervals(splitInFreq2, splitInFreq3, 60)
    val splitIntervals4 = EdmCoreUtils.intervals(splitInFreq3, splitInFreq4, 15)
    val splitIntervals5 = EdmCoreUtils.intervals(splitInFreq4, splitInFreq5, 60)
    val firstDayIntervals = splitIntervals1 ++ splitIntervals2 ++ splitIntervals3 ++ splitIntervals4 ++ splitIntervals5

    val adaIntervals = EdmCoreUtils.extendIntervals(firstDayIntervals, 28)
  }

  trait WithUsers {

    val user1 = User("", "4200301", 96601)
    val user2 = User("", "4200302", 96602)
    val user3 = User("", "4200303", 96603)
  }

  trait WithMobilityMatrixItems extends WithIntervals with WithUsers {

    val item1 = MobilityMatrixItem(adaIntervals(0), adaIntervals(1), "l1", "l2", new Duration(2460000L), 2, user1, 1, 1)
    val item2 = item1.copy(journeyDuration = new Duration(1860000L), origWeight = 0.5, destWeight = 0.5, user = user2)
    val item3 = item2.copy(startLocation = "l2", endLocation = "l2")
    val item4 = item1.copy(
      startInterval = adaIntervals(179),
      endInterval = adaIntervals(180),
      startLocation = "l3",
      endLocation = "l4",
      journeyDuration = new Duration(900000L),
      origWeight = 0.75,
      destWeight = 0.75)
    val item5 = item1.copy(startInterval = adaIntervals(45), endInterval = adaIntervals(46), user = user3)
    val item6 = item1.copy(startInterval = adaIntervals(270), endInterval = adaIntervals(271))

    val items = sc.parallelize(Array(item1, item2, item3, item4, item5, item6))
  }

  trait WithMobilityMatrixItemRows {

    val startDate = EdmCoreUtils.Fmt.parseDateTime("2014/11/02 00:00:00")
    val endDate = startDate.plusHours(3)
    val intervals = EdmCoreUtils.intervals(startDate, endDate, 60)

    val row1 =
      Row(intervals(0).getStart.getZone.getID, intervals(0).getStartMillis, intervals(0).getEnd.getMillis,
        intervals(1).getStartMillis, intervals(1).getEndMillis,
        "loc1", "loc2", 1800000L, 4, Row("", "4200301", 0L), 0.4, 0.6)
    val row2 =
      Row(intervals(0).getStart.getZone.getID, intervals(0).getStartMillis, intervals(0).getEndMillis,
        intervals(1).getStartMillis, intervals(1).getEndMillis,
        "l1", "l2", 2460000L, 0, Row("", "4200302", 0L), 1D, 1D)
    val wrongRow =
      Row("", intervals(1), "loc1", "loc2", new Duration(1800000L), 4, Row("", "4200301", 0), 0.4, 0.6)

    val rows = sc.parallelize(List(row1, row2))
  }

  trait WithMobilityMatrixViews extends WithMobilityMatrixItems {

    val viewItemSunWedLoc1Loc2 =
      MobilityMatrixView(
        startIntervalInitTime = "Sun-Wed 00:00:00",
        endIntervalInitTime = "Sun-Wed 01:00:00",
        startLocation = "l1",
        endLocation = "l2",
        sumWeightedJourneyDurationInSeconds = 5850L,
        sumWeight = 2.5,
        numPeriods = 8,
        users = Set(user1, user2, user3))
    val viewItemKeepSameLocation =
      MobilityMatrixView(
        startIntervalInitTime = "Sun-Wed 00:00:00",
        endIntervalInitTime = "Sun-Wed 01:00:00",
        startLocation = "l2",
        endLocation = "l2",
        sumWeightedJourneyDurationInSeconds = 930L,
        sumWeight = 0.5,
        numPeriods = 8,
        users = Set(user2))
    val viewItemWedThuLoc3Loc4 =
      MobilityMatrixView(
        startIntervalInitTime = "Sun-Wed 23:00:00",
        endIntervalInitTime = "Thu 00:00:00",
        startLocation = "l3",
        endLocation = "l4",
        sumWeightedJourneyDurationInSeconds = 675L,
        sumWeight = 0.75,
        numPeriods = 2,
        users = Set(user1))
    val viewItemSat =
      MobilityMatrixView(
        startIntervalInitTime = "Sat 00:00:00",
        endIntervalInitTime = "Sat 01:00:00",
        startLocation = "l1",
        endLocation = "l2",
        sumWeightedJourneyDurationInSeconds = 2460L,
        sumWeight = 1,
        numPeriods = 2,
        users = Set(user1))

    val perDayOfWeek1 =
      MobilityMatrixView(
        startIntervalInitTime = "Sun 00:00:00",
        endIntervalInitTime = "Sun 01:00:00",
        startLocation = "l1",
        endLocation = "l2",
        sumWeightedJourneyDurationInSeconds = 3390L,
        sumWeight = 1.5,
        numPeriods = 2,
        users = Set(user1, user2))
    val perDayOfWeek2 =
      MobilityMatrixView(
        startIntervalInitTime = "Wed 23:00:00",
        endIntervalInitTime = "Thu 00:00:00",
        startLocation = "l3",
        endLocation = "l4",
        sumWeightedJourneyDurationInSeconds = 675L,
        sumWeight = 0.75,
        numPeriods = 2,
        users = Set(user1))
    val perDayOfWeek3 =
      MobilityMatrixView(
        startIntervalInitTime = "Mon 00:00:00",
        endIntervalInitTime = "Mon 01:00:00",
        startLocation = "l1",
        endLocation = "l2",
        sumWeightedJourneyDurationInSeconds = 2460L,
        sumWeight = 1,
        numPeriods = 2,
        users = Set(user3))
    val perDayOfWeek4 =
      MobilityMatrixView(
        startIntervalInitTime = "Sat 00:00:00",
        endIntervalInitTime = "Sat 01:00:00",
        startLocation = "l1",
        endLocation = "l2",
        sumWeightedJourneyDurationInSeconds = 2460L,
        sumWeight = 1,
        numPeriods = 2,
        users = Set(user1))

    val perDay1 =
      MobilityMatrixView(
        startIntervalInitTime = "00:00:00",
        endIntervalInitTime = "01:00:00",
        startLocation = "l1",
        endLocation = "l2",
        sumWeightedJourneyDurationInSeconds = 8310L,
        sumWeight = 3.5,
        numPeriods = 14,
        users = Set(user1, user2, user3))
    val perDay2 =
      MobilityMatrixView(
        startIntervalInitTime = "23:00:00",
        endIntervalInitTime = "00:00:00",
        startLocation = "l3",
        endLocation = "l4",
        sumWeightedJourneyDurationInSeconds = 675L,
        sumWeight = 0.75,
        numPeriods = 14,
        users = Set(user1))

    val viewItemsNoMinUsersKeepSameLocs =
      List(viewItemSunWedLoc1Loc2, viewItemKeepSameLocation, viewItemWedThuLoc3Loc4, viewItemSat)
    val viewItemsNoMinUsersRemoveSameLocs = List(viewItemSunWedLoc1Loc2, viewItemWedThuLoc3Loc4, viewItemSat)
    val viewItemsOneMinUser = List()
    val viewItemsPerDayOfWeek = List(perDayOfWeek1, perDayOfWeek2, perDayOfWeek3, perDayOfWeek4)
    val viewItemsPerDay = List(perDay1, perDay2)
  }

  trait WithSubscribersProfilingView {

    val subscriberProfilingView1 =
      SubscriberProfilingView(
        imsi = "4200301",
        category =
          ProfilingCategory(
            ageGroup = "16-25",
            genderGroup = "M",
            nationalityGroup = "Saudi Arabia",
            affluenceGroup = "Top 20%"))
    val subscriberProfilingView2 =
      SubscriberProfilingView(
        imsi = "4200302",
        category =
          ProfilingCategory(
            ageGroup = "25-60",
            genderGroup = "F",
            nationalityGroup = "Roamers",
            affluenceGroup = "Middle 30%"))
    val subscriberProfilingView3 =
      SubscriberProfilingView(
        imsi = "4200303",
        category =
          ProfilingCategory(
            ageGroup = "60-",
            genderGroup = "F",
            nationalityGroup = "Non-Saudi",
            affluenceGroup = "Bottom 50%"))

    val subscribersProfilingView =
      sc.parallelize(Array(subscriberProfilingView1, subscriberProfilingView2, subscriberProfilingView3))
  }

  "MobilityMatrixDsl" should "group items using ADA day groups, no minimum users per journey and " +
    "keeping same location journeys" in new WithMobilityMatrixViews {
    items.perDayGroups(
      timeBin = MobilityMatrixView.adaTimeBin,
      numDaysWithinOneWeek = MobilityMatrixView.adaNumDaysWithinOneWeek,
      minUsersPerJourney = 0,
      keepJourneysBetweenSameLocations = true).collect should contain theSameElementsAs(viewItemsNoMinUsersKeepSameLocs)
  }

  it should "group items using ADA day groups, no minimum users per journey and removing same location journeys" in
    new WithMobilityMatrixViews {
      items.perDayGroups(
        timeBin = MobilityMatrixView.adaTimeBin,
        numDaysWithinOneWeek = MobilityMatrixView.adaNumDaysWithinOneWeek,
        minUsersPerJourney = 0).collect should contain theSameElementsAs(viewItemsNoMinUsersRemoveSameLocs)
    }

  it should "group items using ADA day groups, with a minimum number of users per journey" in
    new WithMobilityMatrixViews {
      items.perDayGroups(
        timeBin = MobilityMatrixView.adaTimeBin,
        numDaysWithinOneWeek = MobilityMatrixView.adaNumDaysWithinOneWeek,
        minUsersPerJourney = 1).collect should contain theSameElementsAs(viewItemsOneMinUser)
    }

  it should "group items per day of week" in new WithMobilityMatrixViews {
    items.perDayOfWeek(minUsersPerJourney = 0).collect should contain theSameElementsAs(viewItemsPerDayOfWeek)
  }

  it should "group items per day" in new WithMobilityMatrixViews {
    items.perDay(minUsersPerJourney = 0).collect should contain theSameElementsAs(viewItemsPerDay)
  }

  it should "get correctly parsed rows" in new WithMobilityMatrixItemRows {
    rows.toMobilityMatrixItem.count should be (2)
  }

  it should "save in parquet" in new WithMobilityMatrixItems {
    val path = File.makeTemp().name
    items.saveAsParquetFile(path)
    sqc.parquetFile(path).toMobilityMatrixItem.collect should be (items.collect)
    File(path).deleteRecursively
  }

  it should "filter items belonging to a certain profiling category" in
    new WithMobilityMatrixItems with WithSubscribersProfilingView {
      val targetCategory = (category: ProfilingCategory) =>
        category == ProfilingCategory("16-25", "M", "Saudi Arabia", "Top 20%")
      items.forProfilingCategory(targetCategory, subscribersProfilingView).collect should
        contain theSameElementsAs(List(item1, item4, item6))
    }
}
