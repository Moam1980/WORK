/*
 * TODO: License goes here!
 */

package sa.com.mobily.mobility.spark

import sa.com.mobily.crm.SubscriberProfilingView

import scala.reflect.io.File

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest._

import sa.com.mobily.mobility._
import sa.com.mobily.poi.{Home, Other, Work}
import sa.com.mobily.user.User
import sa.com.mobily.utils.{EdmCoreUtils, LocalSparkSqlContext}

class MobilityMatrixItemTripPurposeDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import MobilityMatrixItemTripPurposeDsl._

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

  trait WithMobilityMatrixItemTripPurposes extends WithIntervals with WithUsers {

    val mobilityMatrixItem1 =
      MobilityMatrixItem(adaIntervals(0), adaIntervals(1), "l1", "l2", new Duration(2460000L), 2, user1, 1, 1)
    val item1 =
      MobilityMatrixItemTripPurpose(
        mobilityMatrixItemParquet = MobilityMatrixItemParquet(mobilityMatrixItem1),
        startLocationType = LocationType(Home, 0.6D),
        endLocationType = LocationType(Work, 0.34D))
    val mobilityMatrixItem2 =
      mobilityMatrixItem1.copy(
        journeyDuration = new Duration(1860000L),
        origWeight = 0.5,
        destWeight = 0.5,
        user = user2)
    val item2 =
      MobilityMatrixItemTripPurpose(
        mobilityMatrixItemParquet = MobilityMatrixItemParquet(mobilityMatrixItem2),
        startLocationType = LocationType(Other, 0.8D),
        endLocationType = LocationType(Home, 0.21D))
    val mobilityMatrixItem3 = mobilityMatrixItem2.copy(startLocation = "l2", endLocation = "l2")
    val item3 =
      MobilityMatrixItemTripPurpose(
        mobilityMatrixItemParquet = MobilityMatrixItemParquet(mobilityMatrixItem3),
        startLocationType = LocationType(Work, 0.21D),
        endLocationType = LocationType(Work, 0.21D))
    val mobilityMatrixItem4 =
      mobilityMatrixItem1.copy(
        startInterval = adaIntervals(179),
        endInterval = adaIntervals(180),
        startLocation = "l3",
        endLocation = "l4",
        journeyDuration = new Duration(900000L),
        origWeight = 0.75,
        destWeight = 0.75)
    val item4 =
      MobilityMatrixItemTripPurpose(
        mobilityMatrixItemParquet = MobilityMatrixItemParquet(mobilityMatrixItem4),
        startLocationType = LocationType(Other, 0.92D),
        endLocationType = LocationType(Other, 0.88D))
    val mobilityMatrixItem5 =
      mobilityMatrixItem1.copy(startInterval = adaIntervals(45), endInterval = adaIntervals(46), user = user3)
    val item5 =
      MobilityMatrixItemTripPurpose(
        mobilityMatrixItemParquet = MobilityMatrixItemParquet(mobilityMatrixItem5),
        startLocationType = LocationType(Work, 0.44D),
        endLocationType = LocationType(Home, 0.18D))
    val mobilityMatrixItem6 =
      mobilityMatrixItem1.copy(startInterval = adaIntervals(270), endInterval = adaIntervals(271))
    val item6 =
      MobilityMatrixItemTripPurpose(
        mobilityMatrixItemParquet = MobilityMatrixItemParquet(mobilityMatrixItem6),
        startLocationType = LocationType(Home, 0.6D),
        endLocationType = LocationType(Work, 0.34D))

    val items = sc.parallelize(Array(item1, item2, item3, item4, item5, item6))

    val subscriberProfilingView1 =
      SubscriberProfilingView(imsi = user1.imsi,
        ageGroup = "16-25",
        genderGroup = "M",
        nationalityGroup = "Saudi Arabia",
        affluenceGroup = "Top 20%")

    val subscriberProfilingView2 =
      SubscriberProfilingView(imsi = user2.imsi,
        ageGroup = "16-25",
        genderGroup = "M",
        nationalityGroup = "Saudi Arabia",
        affluenceGroup = "Top 20%")

    val subscriberProfilingView3 =
      SubscriberProfilingView(imsi = user3.imsi,
        ageGroup = "16-25",
        genderGroup = "M",
        nationalityGroup = "Saudi Arabia",
        affluenceGroup = "Top 20%")

    val subscriberProfilingViews =
      sc.parallelize(Array(subscriberProfilingView1, subscriberProfilingView2, subscriberProfilingView3))
  }

  trait WithMobilityMatrixItemTripPurposeRows extends WithIntervals {

    val locationTypeRow1 = Row(Row(Home.identifier), 0.6D)
    val locationTypeRow2 = Row(Row(Work.identifier), 0.34D)
    val locationTypeRow3 = Row(Row(Other.identifier), 0.91D)
    val locationTypeRow4 = Row(Row(Home.identifier), 0.26D)

    val mobilityMatrixItemParquetRow1 =
      Row(adaIntervals(0).getStart.getZone.getID, adaIntervals(0).getStartMillis, adaIntervals(0).getEndMillis,
        adaIntervals(1).getStartMillis, adaIntervals(1).getEndMillis, "loc1", "loc2", 1800000L, 4,
        Row("", "4200301", 0L), 0.4, 0.6)

    val mobilityMatrixItemParquetRow2 =
      Row(adaIntervals(3).getStart.getZone.getID, adaIntervals(3).getStartMillis, adaIntervals(3).getEndMillis,
        adaIntervals(4).getStartMillis, adaIntervals(4).getEndMillis, "loc3", "loc4", 1800000L, 4,
        Row("", "4200302", 0L), 0.11, 0.74)

    val row1 = Row(mobilityMatrixItemParquetRow1, locationTypeRow1, locationTypeRow2)
    val row2 = Row(mobilityMatrixItemParquetRow2, locationTypeRow3, locationTypeRow4)

    val rows = sc.parallelize(List(row1, row2))
  }

  "MobilityMatrixItemTripPurpose" should "get correctly parsed rows" in new WithMobilityMatrixItemTripPurposeRows {
    rows.toMobilityMatrixItemTripPurpose.count should be (2)
  }

  it should "save in parquet" in new WithMobilityMatrixItemTripPurposes {
    val path = File.makeTemp().name
    items.saveAsParquetFile(path)
    sqc.parquetFile(path).toMobilityMatrixItemTripPurpose.collect should be (items.collect)
    File(path).deleteRecursively
  }

  it should "filter by trip purpose Home to Work" in new WithMobilityMatrixItemTripPurposes {
    val itemsFiltered = items.filterBySubscribersAndTripPurpose(subscriberProfilingViews, HomeToWork)
    itemsFiltered.collect should contain theSameElementsAs(List(item1, item5, item6))
  }

  it should "filter by trip purpose Home to Work and subscriber 1" in new WithMobilityMatrixItemTripPurposes {
    val subscriberProfilingView = sc.parallelize(Array(subscriberProfilingView1))
    val itemsFiltered = items.filterBySubscribersAndTripPurpose(subscriberProfilingView, HomeToWork)
    itemsFiltered.collect should contain theSameElementsAs(List(item1, item6))
  }

  it should "filter by trip purpose Home to Work and subscriber 3" in new WithMobilityMatrixItemTripPurposes {
    val subscriberProfilingView = sc.parallelize(Array(subscriberProfilingView3))
    val itemsFiltered = items.filterBySubscribersAndTripPurpose(subscriberProfilingView, HomeToWork)
    itemsFiltered.collect should contain theSameElementsAs(List(item5))
  }

  it should "filter by trip purpose Home to Work and subscriber 2" in new WithMobilityMatrixItemTripPurposes {
    val subscriberProfilingView = sc.parallelize(Array(subscriberProfilingView2))
    val itemsFiltered = items.filterBySubscribersAndTripPurpose(subscriberProfilingView, HomeToWork)
    itemsFiltered.count should be (0)
  }

  it should "filter by trip purpose Home to Other" in new WithMobilityMatrixItemTripPurposes {
    val itemsFiltered = items.filterBySubscribersAndTripPurpose(subscriberProfilingViews, HomeToOther)
    itemsFiltered.collect should contain theSameElementsAs(List(item2))
  }

  it should "filter by trip purpose Home to Other and subscribers 1 and 3" in new WithMobilityMatrixItemTripPurposes {
    val subscriberProfilingView = sc.parallelize(Array(subscriberProfilingView1, subscriberProfilingView3))
    val itemsFiltered = items.filterBySubscribersAndTripPurpose(subscriberProfilingView, HomeToOther)
    itemsFiltered.count should be(0)
  }

  it should "filter by trip purpose non home based" in new WithMobilityMatrixItemTripPurposes {
    val itemsFiltered = items.filterBySubscribersAndTripPurpose(subscriberProfilingViews, NonHomeBased)
    itemsFiltered.collect should contain theSameElementsAs(List(item3, item4))
  }

  it should "filter by trip purpose non home based and subscriber 1" in new WithMobilityMatrixItemTripPurposes {
    val subscriberProfilingView = sc.parallelize(Array(subscriberProfilingView1))
    val itemsFiltered = items.filterBySubscribersAndTripPurpose(subscriberProfilingView, NonHomeBased)
    itemsFiltered.collect should contain theSameElementsAs(List(item4))
  }

  it should "filter by trip purpose non home based and subscriber 2" in new WithMobilityMatrixItemTripPurposes {
    val subscriberProfilingView = sc.parallelize(Array(subscriberProfilingView2))
    val itemsFiltered = items.filterBySubscribersAndTripPurpose(subscriberProfilingView, NonHomeBased)
    itemsFiltered.collect should contain theSameElementsAs(List(item3))
  }

  it should "filter by trip purpose non home based and subscriber 3" in new WithMobilityMatrixItemTripPurposes {
    val subscriberProfilingView = sc.parallelize(Array(subscriberProfilingView3))
    val itemsFiltered = items.filterBySubscribersAndTripPurpose(subscriberProfilingView, NonHomeBased)
    itemsFiltered.count should be (0)
  }
}
