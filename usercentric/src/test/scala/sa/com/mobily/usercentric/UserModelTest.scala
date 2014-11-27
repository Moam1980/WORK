/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell.{Micro, FourGFdd, Cell}
import sa.com.mobily.event.Event
import sa.com.mobily.geometry.UtmCoordinates
import sa.com.mobily.user.User

class UserModelTest extends FlatSpec with ShouldMatchers {

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

    val slot1 = SpatioTemporalSlot(
      userId = 1,
      startTime = 1,
      endTime = 2,
      geomWkt = "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))",
      events = List(event1))
  }

  "UserModel" should "return an empty list with no events" in new WithSpatioTemporalSlot with WithCellCatalogue {
    UserModel.aggSameCell(List()) should be (List())
  }

  it should "return a single spatio-temporal item with a list with a single element" in
    new WithSpatioTemporalSlot with WithCellCatalogue {
      UserModel.aggSameCell(List(event1)) should be (List(SpatioTemporalSlot(event1)))
    }

  it should "return different spatio-temporal items with a list with no events with the same cell" in
    new WithSpatioTemporalSlot with WithCellCatalogue {
      UserModel.aggSameCell(List(event1, event2, event3)) should
        be (List(SpatioTemporalSlot(event1), SpatioTemporalSlot(event2), SpatioTemporalSlot(event3)))
    }

  it should "aggregate consecutive events having the same cell" in
    new WithSpatioTemporalSlot with WithCellCatalogue {
      UserModel.aggSameCell(List(event1, event1, event1, event2)) should
        be (List(slot1.copy(events = List(event1, event1, event1)), SpatioTemporalSlot(event2)))
    }
}
