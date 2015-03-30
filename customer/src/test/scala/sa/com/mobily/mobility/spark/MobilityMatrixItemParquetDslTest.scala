/*
 * TODO: License goes here!
 */

package sa.com.mobily.mobility.spark

import scala.reflect.io.File

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest._

import sa.com.mobily.location.LocationPoiView
import sa.com.mobily.mobility._
import sa.com.mobily.poi.{Home, Other, Work}
import sa.com.mobily.user.User
import sa.com.mobily.utils.{EdmCoreUtils, LocalSparkSqlContext}

class MobilityMatrixItemParquetDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import MobilityMatrixItemParquetDsl._

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

  trait WithMobilityMatrixItemParquets extends WithIntervals with WithUsers {

    val mobilityMatrixItem1 =
      MobilityMatrixItem(adaIntervals(0), adaIntervals(1), "l1", "l2", new Duration(2460000L), 2, user1, 1, 1)
    val item1 = MobilityMatrixItemParquet(mobilityMatrixItem1)
    val item2 =
      MobilityMatrixItemParquet(
        mobilityMatrixItem1.copy(
          journeyDuration = new Duration(900000L),
          origWeight = 0.5,
          destWeight = 0.5,
          user = user2))
    val item3 = item2.copy(startLocation = "l2", endLocation = "l2")
    val item4 =
      MobilityMatrixItemParquet(
        mobilityMatrixItem1.copy(
          startInterval = adaIntervals(179),
          endInterval = adaIntervals(180),
          startLocation = "l3",
          endLocation = "l4",
          journeyDuration = new Duration(900000L),
          origWeight = 0.75,
          destWeight = 0.75))
    val item5 =
      MobilityMatrixItemParquet(
        mobilityMatrixItem1.copy(startInterval = adaIntervals(45), endInterval = adaIntervals(46), user = user3))
    val item6 =
      MobilityMatrixItemParquet(
        mobilityMatrixItem1.copy(startInterval = adaIntervals(270), endInterval = adaIntervals(271)))
    
    val items = sc.parallelize(Array(item1, item2, item3, item4, item5, item6))
  }

  trait WithLocationPoiViews extends WithMobilityMatrixItemParquets {

    val locationPoiView1 = LocationPoiView(
      imsi = user1.imsi,
      mcc = "420",
      name = "l1",
      poiType = Home,
      weight = 0.34D)
    val locationPoiView2 = LocationPoiView(
      imsi = user1.imsi,
      mcc = "420",
      name = "l2",
      poiType = Work,
      weight = 0.55D)
    val locationPoiView3 = LocationPoiView(
      imsi = user2.imsi,
      mcc = "420",
      name = "l1",
      poiType = Home,
      weight = 0.72D)
    val locationPoiView4 = LocationPoiView(
      imsi = user3.imsi,
      mcc = "420",
      name = "l2",
      poiType = Work,
      weight = 0.12D)

    val mmitp1 =
      MobilityMatrixItemTripPurpose(
        mobilityMatrixItemParquet = item1,
        startLocationType = LocationType(Some(locationPoiView1)),
        endLocationType = LocationType(Some(locationPoiView2)))
    val mmitp2 =
      MobilityMatrixItemTripPurpose(
        mobilityMatrixItemParquet = item2,
        startLocationType = LocationType(Some(locationPoiView3)),
        endLocationType = LocationType(None))
    val mmitp3 =
      MobilityMatrixItemTripPurpose(
        mobilityMatrixItemParquet = item3,
        startLocationType = LocationType(None),
        endLocationType = LocationType(None))
    val mmitp4 =
      MobilityMatrixItemTripPurpose(
        mobilityMatrixItemParquet = item4,
        startLocationType = LocationType(None),
        endLocationType = LocationType(None))
    val mmitp5 =
      MobilityMatrixItemTripPurpose(
        mobilityMatrixItemParquet = item5,
        startLocationType = LocationType(None),
        endLocationType = LocationType(Some(locationPoiView4)))
    val mmitp6 =
      MobilityMatrixItemTripPurpose(
        mobilityMatrixItemParquet = item6,
        startLocationType = LocationType(Some(locationPoiView1)),
        endLocationType = LocationType(Some(locationPoiView2)))

    val locationPoiViews =
      sc.parallelize(Array(locationPoiView1, locationPoiView2, locationPoiView3, locationPoiView4))

    val mmitps = sc.parallelize(Array(mmitp1, mmitp2, mmitp3, mmitp4, mmitp5, mmitp6))
  }

  trait WithMobilityMatrixItemParquetRows extends WithIntervals {

    val row1 =
      Row(adaIntervals(0).getStart.getZone.getID, adaIntervals(0).getStartMillis, adaIntervals(0).getEndMillis,
        adaIntervals(1).getStartMillis, adaIntervals(1).getEndMillis, "loc1", "loc2", 1800000L, 4,
        Row("", "4200301", 0L), 0.4, 0.6)

    val row2 =
      Row(adaIntervals(3).getStart.getZone.getID, adaIntervals(3).getStartMillis, adaIntervals(3).getEndMillis,
        adaIntervals(4).getStartMillis, adaIntervals(4).getEndMillis, "loc3", "loc4", 1800000L, 4,
        Row("", "4200302", 0L), 0.11, 0.74)

    val rows = sc.parallelize(List(row1, row2))
  }

  "MobilityMatrixItemParquet" should "get correctly parsed rows" in new WithMobilityMatrixItemParquetRows {
    rows.toMobilityMatrixItemParquet.count should be (2)
  }

  it should "save in parquet" in new WithMobilityMatrixItemParquets {
    val path = File.makeTemp().name
    items.saveAsParquetFile(path)
    sqc.parquetFile(path).toMobilityMatrixItemParquet.collect should be (items.collect)
    File(path).deleteRecursively
  }

  it should "generate mobility matrix item trip purpose with same elements" in new WithLocationPoiViews {
    items.toMobilityMatrixItemTripPurpose(locationPoiViews).count should be (6)
  }

  it should "generate mobility matrix item trip purpose with correct elements" in new WithLocationPoiViews {
    items.toMobilityMatrixItemTripPurpose(locationPoiViews).collect should contain theSameElementsAs(mmitps.collect)
  }
}
