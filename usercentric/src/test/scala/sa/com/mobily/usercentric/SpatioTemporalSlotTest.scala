/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell.{Micro, FourGFdd, Cell}
import sa.com.mobily.event.{PsEventSource, Event}
import sa.com.mobily.geometry.{UtmCoordinates, Coordinates, GeomUtils}
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCustomMatchers

class SpatioTemporalSlotTest extends FlatSpec with ShouldMatchers with EdmCustomMatchers {

  trait WithEvents {

    val event1 = Event(
      User(imei = "1", imsi = "1", msisdn = 1),
      beginTime = 1,
      endTime = 2,
      lacTac = 1,
      cellId = 1,
      source = PsEventSource,
      eventType = Some("859"),
      subsequentLacTac = Some(0),
      subsequentCellId = Some(0),
      inSpeed = Some(0),
      outSpeed = Some(0.5),
      minSpeedPointWkt = Some("POINT (1 1)"))
    val event2 = event1.copy(beginTime = 3, endTime = 4, cellId = 2, inSpeed = Some(0.5))
    val event3 = event1.copy(beginTime = 5, endTime = 6, cellId = 3, outSpeed = Some(1))
    val event4 = event1.copy(beginTime = 2, endTime = 3, cellId = 3, inSpeed = Some(1))
  }

  trait WithCellCatalogue {

    val cell1 = Cell(1, 1, UtmCoordinates(1, 4), FourGFdd, Micro, 20, 180, 45, 4, "1",
      "POLYGON ((0 0, 0 20, 20 20, 20 0, 0 0))")
    val cell2 = cell1.copy(cellId = 2, coverageWkt = "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))")
    val cell3 = cell1.copy(cellId = 3, coverageWkt = "POLYGON ((5 0, 5 10, 15 10, 15 0, 5 0))")
    val cell4 = cell1.copy(cellId = 4, coverageWkt = "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))")
    val cell5 = cell1.copy(cellId = 5, coverageWkt = "POLYGON ((0.5 0, 0.5 20, 10 20, 10 0, 0.5 0))")

    implicit val cellCatalogue =
      Map((1, 1) -> cell1, (1, 2) -> cell2, (1, 3) -> cell3, (1, 4) -> cell4, (1, 5) -> cell5)
  }

  trait WithSpatioTemporalSlots extends WithEvents {

    val slotWithTwoEvents = SpatioTemporalSlot(
      user = User("", "", 1),
      startTime = 1,
      endTime = 4,
      cells = Set((1, 1), (1, 2)),
      firstEventBeginTime = 1,
      lastEventEndTime = 4,
      outMinSpeed = 0,
      intraMinSpeedSum = 0.5,
      numEvents = 2)
    val event3SpatioTemporalSlot = SpatioTemporalSlot(
      user = User("", "", 1),
      startTime = 5,
      endTime = 6,
      cells = Set((1, 3)),
      firstEventBeginTime = 5,
      lastEventEndTime = 6,
      outMinSpeed = 1,
      intraMinSpeedSum = 0,
      numEvents = 1)
    val slotWithThreeEvents = SpatioTemporalSlot(
      user = User("", "", 1),
      startTime = 1,
      endTime = 6,
      cells = Set((1, 1), (1, 2), (1, 3)),
      firstEventBeginTime = 1,
      lastEventEndTime = 6,
      outMinSpeed = 1,
      intraMinSpeedSum = 0.5,
      numEvents = 3)
    val slotWithEmptyIntersection = slotWithTwoEvents.copy(cells = Set((1, 4), (1, 5)))
  }

  "SpatioTemporalSlot" should "build geometry from WKT" in new WithSpatioTemporalSlots with WithCellCatalogue {
    slotWithTwoEvents.geom should
      equalGeometry(GeomUtils.parseWkt("POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))", Coordinates.SaudiArabiaUtmSrid))
  }

  it should "append another event" in new WithSpatioTemporalSlots with WithCellCatalogue {
    slotWithTwoEvents.append(event3) should be (slotWithThreeEvents)
  }

  it should "build from event" in new WithSpatioTemporalSlots with WithCellCatalogue {
    SpatioTemporalSlot(event3) should be (event3SpatioTemporalSlot)
  }

  it should "append another SpatioTemporalSlot" in new WithSpatioTemporalSlots {
    slotWithTwoEvents.append(event3SpatioTemporalSlot) should be (slotWithThreeEvents)
  }

  it should "append another SpatioTemporalSlot with end times before current slot end times" in
    new WithSpatioTemporalSlots {
      slotWithTwoEvents.append(
        event3SpatioTemporalSlot.copy(startTime = 2, endTime = 3, firstEventBeginTime = 2, lastEventEndTime = 3)) should
        be (slotWithThreeEvents.copy(endTime = 4, lastEventEndTime = 4))
    }

  it should "have the proper header" in {
    SpatioTemporalSlot.header should be (
      Array("imei", "imsi", "msisdn", "startTime", "endTime", "numEvents", "outMinSpeed", "avgIntraMinSpeed", "cells",
        "firstEventBeginTime", "lastEventEndTime", "geomWkt", "countryIsoCode", "typeEstimate"))
  }

  it should "return its fields for printing" in new WithSpatioTemporalSlots with WithCellCatalogue {
    slotWithTwoEvents.fields should be (
      Array("", "", "1", "1970/01/01 03:00:00", "1970/01/01 03:00:00", "2", "0.0", "0.5", "(1,1);(1,2)",
        "1970/01/01 03:00:00", "1970/01/01 03:00:00", "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))", "sa", "Dwell"))
  }

  it should "compute the average intra minimum speed when there are several events" in new WithSpatioTemporalSlots {
    slotWithThreeEvents.avgIntraMinSpeed should be (0.25)
  }

  it should "compute the average intra minimum speed when there's only one event" in new WithSpatioTemporalSlots {
    event3SpatioTemporalSlot.avgIntraMinSpeed should be (0)
  }

  it should "compute the seconds in between two slots" in new WithSpatioTemporalSlots {
    SpatioTemporalSlot.secondsInBetween(
      slotWithTwoEvents.copy(endTime = 33000), slotWithTwoEvents.copy(startTime = 48000)) should be (15)
  }

  "CellsGeometry" should "compute geometry from cells" in new WithSpatioTemporalSlots with WithCellCatalogue {
    slotWithTwoEvents.geom should
      equalGeometry(GeomUtils.parseWkt("POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))", Coordinates.SaudiArabiaUtmSrid))
  }

  it should "compute the union when the intersection is empty (and simplify afterwards)" in
    new WithSpatioTemporalSlots with WithCellCatalogue {
      slotWithEmptyIntersection.geom should
        equalGeometry(GeomUtils.parseWkt("POLYGON ((0 0, 0.5 20, 10 20, 10 0, 0 0))", Coordinates.SaudiArabiaUtmSrid))
  }
}
