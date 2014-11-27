/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell.{Micro, FourGFdd, Cell}
import sa.com.mobily.event.Event
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
      eventType = "859",
      subsequentLacTac = Some(0),
      subsequentCellId = Some(0),
      inSpeed = Some(0),
      outSpeed = Some(3),
      minSpeedPointWkt = Some("POINT (1 1)"))
    val event2 = event1.copy(beginTime = 3, endTime = 4, cellId = 2)
    val event3 = event1.copy(beginTime = 5, endTime = 6, cellId = 3)
  }

  trait WithCellCatalogue {

    val cell1 = Cell(1, 1, UtmCoordinates(1, 4), FourGFdd, Micro, 20, 180, 45, 4,
      "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))")
    val cell2 = cell1.copy(cellId = 2, coverageWkt = "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))")
    val cell3 = cell1.copy(cellId = 3, coverageWkt = "POLYGON ((0.5 0, 0.5 1, 1.5 1, 1.5 0, 0.5 0))")

    implicit val cellCatalogue = Map((1, 1) -> cell1, (1, 2) -> cell2, (1, 3) -> cell3)
  }

  trait WithSpatioTemporalSlot extends WithEvents {

    val slotWithTwoEvents = SpatioTemporalSlot(
      userId = 1,
      startTime = 1,
      endTime = 4,
      geomWkt = "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
      events = List(event1, event2))
    val slotWithThreeEvents = SpatioTemporalSlot(
      userId = 1,
      startTime = 1,
      endTime = 6,
      geomWkt = "POLYGON ((0.5 1, 1 1, 1 0, 0.5 0, 0.5 1))",
      events = List(event1, event2, event3))
    val event3SpatioTemporalSlot = SpatioTemporalSlot(
      userId = 1,
      startTime = 5,
      endTime = 6,
      geomWkt = "POLYGON ((0.5 0, 0.5 1, 1.5 1, 1.5 0, 0.5 0))",
      events = List(event3))
  }

  "SpatioTemporalSlot" should "build geometry from WKT" in new WithSpatioTemporalSlot {
    slotWithTwoEvents.geom should
      equalGeometry(GeomUtils.parseWkt("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", Coordinates.SaudiArabiaUtmSrid))
  }

  it should "compute the set of cells seen" in new WithSpatioTemporalSlot {
    slotWithTwoEvents.cells should be (Set((1, 1), (1, 2)))
  }

  it should "append another event" in new WithSpatioTemporalSlot with WithCellCatalogue {
    slotWithTwoEvents.append(event3) should be (slotWithThreeEvents)
  }

  it should "build from event" in new WithSpatioTemporalSlot with WithCellCatalogue {
    SpatioTemporalSlot(event3) should be (event3SpatioTemporalSlot)
  }
}
