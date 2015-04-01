/*
 * TODO: License goes here!
 */

package sa.com.mobily.mobility

import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest._

import sa.com.mobily.location.LocationPoiView
import sa.com.mobily.poi.{Home, Other, Work}
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils

class MobilityMatrixItemTripPurposeTest extends FlatSpec with ShouldMatchers {

  import MobilityMatrixItemTripPurpose._

  trait WithLocationTypes {

    val locationTypeRow1 = Row(Row(Home.identifier), 0.6D)
    val locationTypeRow2 = Row(Row(Work.identifier), 0.34D)
    val locationTypeRow3 = Row(Row(Other.identifier), 1D)

    val locationTypeWrongRow1 = Row(Home.identifier, 1D)
    val locationTypeWrongRow2 = Row(Row(Home.identifier), "NaN")

    val locationType1 = LocationType(Home, 0.6D)
    val locationType2 = LocationType(Work, 0.34D)
    val locationType3 = LocationType(Other, 1D)

    val locationTypeFields1 = Array(Home.identifier, "0.6")
    val locationTypeFields2 = Array(Work.identifier, "0.34")
    val locationTypeFields3 = Array(Other.identifier, "1.0")
  }

  trait WithLocationPoiViews extends WithLocationTypes {

    val locationPoiView1 =
      LocationPoiView(
        imsi = "4200301",
        mcc = "420",
        name = "loc1",
        poiType = Home,
        weight = 0.6D)

    val locationPoiView2 =
      LocationPoiView(
        imsi = "4200301",
        mcc = "420",
        name = "loc2",
        poiType = Work,
        weight = 0.34D)

    val locationPoiView3 =
      LocationPoiView(
        imsi = "4200301",
        mcc = "420",
        name = "loc3",
        poiType = Other,
        weight = 1.0D)
  }

  trait WithIntervals {

    val startDate = EdmCoreUtils.Fmt.parseDateTime("2014/11/02 00:00:00")
    val endDate = startDate.plusHours(3)
    val intervals = EdmCoreUtils.intervals(startDate, endDate, 60)
  }

  trait WithMobilityMatrixItemTripPurposes extends WithIntervals with WithLocationTypes {

    val mobilityMatrixItemParquetRow =
      Row(intervals(0).getStart.getZone.getID, intervals(0).getStartMillis, intervals(0).getEndMillis,
        intervals(1).getStartMillis, intervals(1).getEndMillis, "loc1", "loc2", 1800000L, 4, Row("", "4200301", 0L),
        0.4, 0.6)

    val row = Row(mobilityMatrixItemParquetRow, locationTypeRow1, locationTypeRow2)
    val wrongRow =
      Row("", intervals(1).getStartMillis, intervals(1).getEndMillis, "loc1", "loc2", new Duration(1800000L), 4,
        Row("", "4200301", 0), 0.4, 0.6)

    val mobilityMatrixItemParquet =
      MobilityMatrixItemParquet(
        intervals(0).getStart.getZone.getID,
        intervals(0).getStartMillis,
        intervals(0).getEndMillis,
        intervals(1).getStartMillis,
        intervals(1).getEndMillis,
        "loc1", "loc2", 1800000L, 4, User("", "4200301", 0), 0.4, 0.6)

    val mobilityMatrixItem =
      MobilityMatrixItem(intervals(0), intervals(1), "loc1", "loc2", new Duration(1800000L), 4,
        User("", "4200301", 0), 0.4, 0.6)

    val item =
      MobilityMatrixItemTripPurpose(
        mobilityMatrixItemParquet = mobilityMatrixItemParquet,
        startLocationType = locationType1,
        endLocationType = locationType2)

    val weight = ((0.4 * 0.6) + (0.6 * 0.34)) / 2
    val weightFormula =
      ((mobilityMatrixItem.origWeight * locationType1.weight) +
        (mobilityMatrixItem.destWeight * locationType2.weight)) / 2
  }

  "MobilityMatrixItemTripPurpose" should "location type be built from Row" in new WithLocationTypes {
    LocationType.fromRow.fromRow(locationTypeRow1) should be (locationType1)
    LocationType.fromRow.fromRow(locationTypeRow2) should be (locationType2)
    LocationType.fromRow.fromRow(locationTypeRow3) should be (locationType3)
  }

  it should "location type be discarded when row is wrong" in new WithLocationTypes {
    an[Exception] should be thrownBy LocationType.fromRow.fromRow(locationTypeWrongRow1)
    an[Exception] should be thrownBy LocationType.fromRow.fromRow(locationTypeWrongRow2)
  }

  it should "location type return correct fields" in new WithLocationTypes {
    locationType1.fields should be (locationTypeFields1)
    locationType2.fields should be (locationTypeFields2)
    locationType3.fields should be (locationTypeFields3)
  }

  it should "location type apply from options values" in new WithLocationPoiViews {
    LocationType(Some(locationPoiView1)) should be (locationType1)
    LocationType(Some(locationPoiView2)) should be (locationType2)
    LocationType(Some(locationPoiView3)) should be (locationType3)
  }

  it should "location type apply from options to None" in new WithLocationTypes {
    LocationType(None) should be (LocationType(Other, LocationType.DefaultWeight))
  }

  it should "be built from Row" in new WithMobilityMatrixItemTripPurposes {
    fromRow.fromRow(row) should be (item)
  }

  it should "be discarded when row is wrong" in new WithMobilityMatrixItemTripPurposes {
    an[Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }

  it should "return correct weight using start and end type weight" in new WithMobilityMatrixItemTripPurposes {
    item.weight should be (weight)
    item.weight should be (weightFormula)
  }

  it should "return HomeToWork as trip purpose when origin is home and destination is work" in
    new WithMobilityMatrixItemTripPurposes {
      item.tripPurpose should be (HomeToWork)
    }

  it should "return HomeToWork as trip purpose when origin is work and destination is home" in
    new WithMobilityMatrixItemTripPurposes {
      item.copy(startLocationType = locationType2, endLocationType = locationType1).tripPurpose should be (HomeToWork)
    }

  it should "return HomeToOther as trip purpose when origin is home and destination is other" in
    new WithMobilityMatrixItemTripPurposes {
      item.copy(endLocationType = locationType3).tripPurpose should be (HomeToOther)
    }

  it should "return HomeToOther as trip purpose when origin is other and destination is home" in
    new WithMobilityMatrixItemTripPurposes {
      item.copy(startLocationType = locationType3, endLocationType = locationType1).tripPurpose should be (HomeToOther)
    }

  it should "return NonHomeBased as trip purpose when origin is work and destination is other" in
    new WithMobilityMatrixItemTripPurposes {
      item.copy(startLocationType = locationType2, endLocationType = locationType3).tripPurpose should be (NonHomeBased)
    }

  it should "return HomeToOther as trip purpose when origin is other and destination is work" in
    new WithMobilityMatrixItemTripPurposes {
      item.copy(startLocationType = locationType3, endLocationType = locationType2).tripPurpose should be (NonHomeBased)
    }

  it should "return HomeToOther as trip purpose when origin is other and destination is other" in
    new WithMobilityMatrixItemTripPurposes {
      item.copy(startLocationType = locationType3, endLocationType = locationType3).tripPurpose should be (NonHomeBased)
    }
}
