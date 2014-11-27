/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.event.Event
import sa.com.mobily.geometry.{Coordinates, GeomUtils}
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCustomMatchers

class UserModelTest extends FlatSpec with ShouldMatchers with EdmCustomMatchers {

  trait WithEvents {
    val event1 = Event(
      User(imei = "1", imsi = "1", msisdn = 1),
      beginTime = 1,
      endTime = 2,
      lacTac = 1324,
      cellId = 13067,
      eventType = "859",
      subsequentLacTac = Some(0),
      subsequentCellId = Some(0),
      inSpeed = Some(0),
      outSpeed = Some(3),
      minSpeedPointWkt = Some("POINT (1 1)"))
    val event2 = event1.copy(beginTime = 3, endTime = 4, cellId = 13069)
    val event3 = event1.copy(beginTime = 5, endTime = 6, cellId = 13067)

    val events = List(event1, event2, event3)
  }

  trait WithSpatioTemporalSlot extends WithEvents {
    val slot = SpatioTemporalSlot(
      user = 1,
      startTime = 0,
      endTime = 10,
      geomWkt = "POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))",
      events = events)
  }

  "UserModel" should "build geometry from WKT" in new WithSpatioTemporalSlot {
    slot.geom should
      equalGeometry(GeomUtils.parseWkt("POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0))", Coordinates.SaudiArabiaUtmSrid))
  }

  it should "compute the set of cells seen" in new WithSpatioTemporalSlot {
    slot.cells should be (Set((1324, 13067), (1324, 13069)))
  }
}
