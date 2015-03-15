/*
 * TODO: License goes here!
 */

package sa.com.mobily.mobility

import com.github.nscala_time.time.Imports._
import org.scalatest._

import sa.com.mobily.geometry.Coordinates
import sa.com.mobily.location.Location
import sa.com.mobily.user.User
import sa.com.mobily.usercentric.Dwell
import sa.com.mobily.utils.EdmCoreUtils

class MobilityMatrixItemTest extends FlatSpec with ShouldMatchers {

  trait WithIntervals {

    val startDate = EdmCoreUtils.Fmt.parseDateTime("2014/11/02 00:00:00")
    val endDate = startDate.plusHours(3)
    val intervals = EdmCoreUtils.intervals(startDate, endDate, 60)
  }

  trait WithMobilityMatrixItems extends WithIntervals {

    val item = MobilityMatrixItem(intervals(0), intervals(1), "loc1", "loc2", new Duration(1800000L), 4,
      User("", "4200301", 0), 0.5)

    val itemDwell1And2 =
      MobilityMatrixItem(intervals(0), intervals(1), "l1", "l2", new Duration(2460000L), 0, User("", "4200301", 0), 1)
    val item1Dwell3And4 = MobilityMatrixItem(intervals(0), intervals(1), "l3", "l5", new Duration(2460000L), 0,
      User("", "4200301", 0), 0.20869565217391306)
    val item2Dwell3And4 = MobilityMatrixItem(intervals(0), intervals(1), "l3", "l6", new Duration(2460000L), 0,
      User("", "4200301", 0), 0.1326086956521739)
    val item3Dwell3And4 = MobilityMatrixItem(intervals(0), intervals(1), "l4", "l5", new Duration(2460000L), 0,
      User("", "4200301", 0), 0.15869565217391307)
    val item4Dwell3And4 = MobilityMatrixItem(intervals(0), intervals(1), "l4", "l6", new Duration(2460000L), 0,
      User("", "4200301", 0), 0.08260869565217391)
  }

  trait WithDwells {

    val dwell1 = Dwell(
      user = User("", "4200301", 0),
      startTime = 1414875600000L,
      endTime = 1414876800000L,
      geomWkt = "POLYGON ((2 6, 2 7, 3 7, 3 6, 2 6))",
      cells = Seq((2, 4), (2, 6)),
      firstEventBeginTime = 3,
      lastEventEndTime = 9,
      numEvents = 2)
    val dwell2 = dwell1.copy(
      startTime = 1414879260000L,
      endTime = 1414880460000L,
      geomWkt = "POLYGON ((6 6, 6 7, 7 7, 7 6, 6 6))")
    val dwell3 = dwell1.copy(geomWkt = "POLYGON ((0 0, 0 2.5, 2 2.5, 2 0, 0 0))")
    val dwell4 = dwell2.copy(geomWkt = "POLYGON ((3 0, 3 2.3, 5 2.3, 5 0, 3 0))")

    val shortDwell = dwell1.copy(endTime = 1414875900000L)
  }

  trait WithLocations {

    val l1 = Location(name = "l1", client = "client", epsg = Coordinates.SaudiArabiaUtmEpsg,
      geomWkt = "POLYGON ((1 5, 1 8, 4 8, 4 5, 1 5))")
    val l2 = l1.copy(name = "l2", geomWkt = "POLYGON ((5 5, 5 8, 8 8, 8 5, 5 5))")
    val l3 = l1.copy(name = "l3", geomWkt = "POLYGON ((0 1, 1 1, 2 1, 2 0, 0 1))")
    val l4 = l1.copy(name = "l4", geomWkt = "POLYGON ((1 2, 1 3, 2 3, 2 2, 1 2))")
    val l5 = l1.copy(name = "l5", geomWkt = "POLYGON ((3 0, 3 1, 4 1, 4 0, 3 0))")
    val l6 = l1.copy(name = "l6", geomWkt = "POLYGON ((3 2, 3 3, 4 3, 4 2, 3 2))")

    val locations = List(l1, l2, l3, l4, l5, l6)
  }

  "MobilityMatrixItem" should "return fields" in new WithMobilityMatrixItems {
    item.fields should be (
      Array("2014-11-02 00:00:00", "2014-11-02 01:00:00", "loc1", "loc2", "1800", "4", "", "4200301", "0", "0.5"))
  }

  it should "have same number of fields and header" in new WithMobilityMatrixItems {
    item.fields.size should be (MobilityMatrixItem.Header.size)
  }

  it should "return no mobility matrix items when there are no dwells" in new WithIntervals with WithLocations {
    MobilityMatrixItem.perIntervalAndLocation(List(), intervals, locations, 15, 0) should be (List())
  }

  it should "return no mobility matrix items when there is a single dwell" in
    new WithIntervals with WithLocations with WithDwells {
      MobilityMatrixItem.perIntervalAndLocation(List(dwell1), intervals, locations, 15, 0) should be (List())
    }

  it should "discard dwells with less duration than threshold (no matter in which order they are)" in
    new WithIntervals with WithLocations with WithDwells {
      MobilityMatrixItem.perIntervalAndLocation(List(shortDwell, dwell1), intervals, locations, 15, 0) should
        be (List())
      MobilityMatrixItem.perIntervalAndLocation(List(dwell1, shortDwell), intervals, locations, 15, 0) should
        be (List())
    }

  it should "compute mobility matrix items when locations contain dwells" in
    new WithIntervals with WithLocations with WithDwells with WithMobilityMatrixItems {
      MobilityMatrixItem.perIntervalAndLocation(List(dwell1, dwell2), intervals, locations, 15, 0) should
        be (List(itemDwell1And2))
    }

  it should "compute mobility matrix items when locations intersect dwells in different ways" in
    new WithIntervals with WithLocations with WithDwells with WithMobilityMatrixItems {
      MobilityMatrixItem.perIntervalAndLocation(List(dwell3, dwell4), intervals, locations, 15, 0) should
        contain theSameElementsAs (List(item1Dwell3And4, item2Dwell3And4, item3Dwell3And4, item4Dwell3And4))
    }

  it should "compute mobility matrix items (with short dwells in between)" in
    new WithIntervals with WithLocations with WithDwells with WithMobilityMatrixItems {
      MobilityMatrixItem.perIntervalAndLocation(List(dwell3, shortDwell, dwell4), intervals, locations, 15, 0) should
        contain theSameElementsAs (List(item1Dwell3And4, item2Dwell3And4, item3Dwell3And4, item4Dwell3And4))
    }

  it should "compute mobility matrix items (with short dwells at the beginning)" in
    new WithIntervals with WithLocations with WithDwells with WithMobilityMatrixItems {
      MobilityMatrixItem.perIntervalAndLocation(List(shortDwell, dwell3, dwell4), intervals, locations, 15, 0) should
        contain theSameElementsAs (List(item1Dwell3And4, item2Dwell3And4, item3Dwell3And4, item4Dwell3And4))
    }
}
