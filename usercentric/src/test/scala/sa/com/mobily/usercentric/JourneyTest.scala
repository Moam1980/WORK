/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.operation.distance.DistanceOp
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell.{Micro, FourGFdd, Cell}
import sa.com.mobily.cell.spark.CellDsl._
import sa.com.mobily.event.Event
import sa.com.mobily.geometry.{Coordinates, GeomUtils, UtmCoordinates}
import sa.com.mobily.user.User
import sa.com.mobily.utils.{EdmCustomMatchers, LocalSparkContext}

class JourneyTest extends FlatSpec with ShouldMatchers  with EdmCustomMatchers with LocalSparkContext {

  trait WithEvents {

    val event1 = Event(User("imei", "imsi", 1L), 0, 0, 1, 1, "EventType", None, None, None, None)
    val event20 = event1.copy(beginTime = 5000, endTime = 5000, cellId = 20)
    val event21 = event1.copy(beginTime = 5000, endTime = 5000, cellId = 21)
    val event3 = event1.copy(beginTime = 5000, endTime = 5000, cellId = 3)
    val event101 = event1.copy(beginTime = 0, endTime = 0, cellId = 101)
    val event120 = event1.copy(beginTime = 5000, endTime = 5000, cellId = 120)
    val event1200 = event1.copy(beginTime = 6000, endTime = 6000, cellId = 120)
    val event1201 = event1.copy(beginTime = 8000, endTime = 8000, cellId = 120)
    val event130 = event1.copy(beginTime = 10000, endTime = 10000, cellId = 130)

    val eventIntersect1 = event1.copy(beginTime = 0, endTime = 0, cellId = 1001)
    val eventIntersect2 = event1.copy(beginTime = 4000, endTime = 4000, cellId = 1002)
    val eventIntersect3 = event1.copy(beginTime = 8000, endTime = 8000, cellId = 1003)
    val eventIntersectWith1And2 = event1.copy(beginTime = 2000, endTime = 2000, cellId = 1004)
    val eventIntersectWith2And3 = event1.copy(beginTime = 4000, endTime = 4000, cellId = 1005)
    val eventContainInitPoint1 = event1.copy(beginTime = 2000, endTime = 2000, cellId = 1006)
  }

  trait WithCellCatalogue {

    val cell1 = Cell(1, 1, UtmCoordinates(1, 4), FourGFdd, Micro, 20, 180, 45, 4,
      "POLYGON (( 0 0, 0 4, 2 4, 2 0, 0 0 ))")
    val cell20 = cell1.copy(cellId = 20, planarCoords = UtmCoordinates(7, 4),
      coverageWkt = "POLYGON (( 3 4, 7 4, 7 0, 3 4 ))")
    val cell21 = cell1.copy(cellId = 21, planarCoords = UtmCoordinates(7, 4),
      coverageWkt = "POLYGON (( 3 4, 7 4, 7 0, 5 0, 3 4 ))")
    val cell22 = cell1.copy(cellId = 22, planarCoords = UtmCoordinates(7, 4),
      coverageWkt = "POLYGON (( 3 4, 7 4, 7 0, 4 0, 4 2, 3 2, 3 4 ))")
    val cell3 = cell1.copy(cellId = 3, planarCoords = UtmCoordinates(1, 5),
      coverageWkt = "POLYGON (( 0 5, 0 7, 2 7, 2 5, 0 5 ))")
    val cell101 = cell1.copy(cellId = 101, planarCoords = UtmCoordinates(1, 5),
      coverageWkt = "POLYGON (( 1 0, 1 2, 3 2, 3 0, 1 0 ))")
    val cell120 = cell1.copy(cellId = 120, planarCoords = UtmCoordinates(1, 5),
      coverageWkt = "POLYGON (( 2 3, 2 5, 4 5, 4 3, 2 3 ))")
    val cell130 = cell1.copy(cellId = 130, planarCoords = UtmCoordinates(1, 5),
      coverageWkt = "POLYGON (( 0 6, 0 7, 2 7, 2 6, 0 6 ))")

    val cellIntersect1 = cell1.copy(cellId = 1001, planarCoords = UtmCoordinates(0, 0),
      coverageWkt = "POLYGON (( 0 0, 0 3, 3 3, 3 0, 0 0 ))")
    val cellIntersect2 = cell1.copy(cellId = 1002, planarCoords = UtmCoordinates(4, 0),
      coverageWkt = "POLYGON (( 4 0, 4 3, 7 3, 7 0, 4 0 ))")
    val cellIntersect3 = cell1.copy(cellId = 1003, planarCoords = UtmCoordinates(8, 0),
      coverageWkt = "POLYGON (( 8 0, 8 3, 11 3, 11 0, 8 0 ))")
    val cellIntersectWith1And2 = cell1.copy(cellId = 1004, planarCoords = UtmCoordinates(2, 0),
      coverageWkt = "POLYGON (( 2 0, 2 3, 5 3, 5 0, 2 0 ))")
    val cellIntersectWith2And3 = cell1.copy(cellId = 1005, planarCoords = UtmCoordinates(6, 0),
      coverageWkt = "POLYGON (( 6 0, 6 3, 9 3, 9 0, 6 0 ))")
    val cellContainInitPoint1 = cell1.copy(cellId = 1006, planarCoords = UtmCoordinates(1, 0),
      coverageWkt = "POLYGON (( 1 0, 1 3, 4 3, 4 0, 1 0 ))")

    val cells = sc.parallelize(List(cell1, cell20, cell21, cell22, cell3, cell101, cell120, cell130, cellIntersect1,
      cellIntersect2, cellIntersect3, cellIntersectWith1And2, cellIntersectWith2And3,
      cellContainInitPoint1)).toBroadcastMap.value
  }

  trait WithInitPoints {

    val geomFactory = GeomUtils.geomFactory(Coordinates.SaudiArabiaUtmSrid)
    val initPoint1 = geomFactory.createPoint(new Coordinate(2, 0))
  }

  "Journey" should "provide with cell geometry for an event" in new WithEvents with WithCellCatalogue {
    Journey.eventGeom(cells)(event20) should equalGeometry(cell20.coverageGeom)
  }

  it should "calculate the seconds in between two events" in new WithEvents {
    Journey.secondsInBetween(event1, event20) should be(5)
  }

  it should "calculate the seconds in between two simultaneous events" in new WithEvents {
    Journey.secondsInBetween(event1, event1) should be(0.001)
  }

  it should "assign a zero incoming speed to an event" in new WithEvents {
    Journey.zeroInSpeed(event1).inSpeed should be(Some(0.0))
  }

  it should "assign a zero outgoing speed to an event" in new WithEvents {
    Journey.zeroOutSpeed(event1).outSpeed should be(Some(0.0))
  }

  it should "compute the point for next geometry when there's a path through that geometry towards its next one" in
    new WithEvents with WithCellCatalogue with WithInitPoints {
      val closestInSecondToInit1 =
        geomFactory.createPoint(DistanceOp.nearestPoints(initPoint1, cell20.coverageGeom).last)
      Journey.nextInitPoint(closestInSecondToInit1, cell20.coverageGeom, cell3.coverageGeom, geomFactory) should
        equalGeometry(geomFactory.createPoint(new Coordinate(3.75, 3.25)))
    }

  it should "compute the point for next geometry when there's no path through that geometry towards its next one" in
    new WithEvents with WithCellCatalogue with WithInitPoints {
      val closestInSecondToInit1 =
        geomFactory.createPoint(DistanceOp.nearestPoints(initPoint1, cell21.coverageGeom).last)
      Journey.nextInitPoint(closestInSecondToInit1, cell21.coverageGeom, cell3.coverageGeom, geomFactory) should
        equalGeometry(geomFactory.createPoint(new Coordinate(4.4, 1.2)))
    }

  it should "compute the point for next geometry when there's a partial path through that geometry towards " +
    "its next one" in new WithEvents with WithCellCatalogue with WithInitPoints {
    val closestInSecondToInit1 =
      geomFactory.createPoint(DistanceOp.nearestPoints(initPoint1, cell22.coverageGeom).last)
    Journey.nextInitPoint(closestInSecondToInit1, cell22.coverageGeom, cell3.coverageGeom, geomFactory) should
      equalGeometry(geomFactory.createPoint(new Coordinate(4, 1.25)))
  }

  it should "process an empty list of events" in new WithCellCatalogue {
    Journey.computeMinSpeed(List(), cells) should be(List())
  }

  it should "assign zero in/out speed to a single event list" in new WithCellCatalogue with WithEvents {
    Journey.computeMinSpeed(List(event1), cells) should be(List(event1.copy(inSpeed = Some(0), outSpeed = Some(0))))
  }

  it should "compute minimum speed for a list with two events" in new WithCellCatalogue with WithEvents {
    val eventsWithSpeed = Journey.computeMinSpeed(List(event101, event120), cells)
    eventsWithSpeed.map(_.inSpeed) should be(List(Some(0), Some(0.4)))
    eventsWithSpeed.map(_.outSpeed) should be(List(Some(0.4), Some(0)))
  }

  it should "compute minimum speed for a list with some events belonging to the same cell" in
    new WithCellCatalogue with WithEvents {
      val eventsWithSpeed = Journey.computeMinSpeed(List(event101, event120, event1200, event1201, event130), cells)
      eventsWithSpeed.map(_.inSpeed) should be(List(Some(0.0), Some(0.4), Some(0.0), Some(0.0), Some(1.5)))
      eventsWithSpeed.map(_.outSpeed) should be(List(Some(0.4), Some(0.0), Some(0.0), Some(1.5), Some(0.0)))
    }

  it should "compute minimum speed for a list with three events (no intersections)" in
    new WithCellCatalogue with WithEvents {
      val eventsWithSpeed = Journey.computeMinSpeed(List(event101, event120, event130), cells)
      eventsWithSpeed.map(_.inSpeed) should be(List(Some(0.0), Some(0.6), Some(0.4)))
      eventsWithSpeed.map(_.outSpeed) should be(List(Some(0.6), Some(0.4), Some(0.0)))
  }

  it should "compute minimum speed when second intersects third" in new WithCellCatalogue with WithEvents {
    val eventsWithSpeed =
      Journey.computeMinSpeed(List(eventIntersect1, eventIntersectWith2And3, eventIntersect3), cells)
    eventsWithSpeed.map(_.inSpeed) should be(List(Some(0.0), Some(1.375), Some(0.25)))
    eventsWithSpeed.map(_.outSpeed) should be(List(Some(1.375), Some(0.25), Some(0.0)))
  }

  it should "compute minimum speed when first intersects second" in new WithCellCatalogue with WithEvents {
    val eventsWithSpeed =
      Journey.computeMinSpeed(List(eventIntersect1, eventIntersectWith1And2, eventIntersect3), cells)
    eventsWithSpeed.map(_.inSpeed) should be(List(Some(0.0), Some(1.0), Some(0.75)))
    eventsWithSpeed.map(_.outSpeed) should be(List(Some(1.0), Some(0.75), Some(0.0)))
  }

  it should "compute minimum speed when first intersects second and second intersects third" in
    new WithCellCatalogue with WithEvents {
      val eventsWithSpeed =
        Journey.computeMinSpeed(List(eventIntersect1, eventIntersectWith1And2, eventIntersect2), cells)
      eventsWithSpeed.map(_.inSpeed) should be(List(Some(0.0), Some(0.75), Some(0.5)))
      eventsWithSpeed.map(_.outSpeed) should be(List(Some(0.75), Some(0.5), Some(0.0)))
    }

  it should "compute minimum speed when initial point is inside second" in new WithCellCatalogue with WithEvents {
    val eventsWithSpeed =
      Journey.computeMinSpeed(List(eventIntersect1, eventContainInitPoint1, eventIntersect2), cells)
    eventsWithSpeed.map(_.inSpeed) should be(List(Some(0.0), Some(0.0), Some(1.25)))
    eventsWithSpeed.map(_.outSpeed) should be(List(Some(0.0), Some(1.25), Some(0.0)))
  }

  it should "compute minimum speed when initial point is in the second's border" in
    new WithCellCatalogue with WithEvents {
      val eventsWithSpeed =
        Journey.computeMinSpeed(List(eventIntersect1, eventIntersect2, eventIntersect2, eventIntersect3), cells)
      eventsWithSpeed.map(_.inSpeed) should be(List(Some(0.0), Some(0.625), Some(0.0), Some(1.0)))
      eventsWithSpeed.map(_.outSpeed) should be(List(Some(0.625), Some(0.0), Some(1.0), Some(0.0)))
    }
}
