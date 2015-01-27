/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.operation.distance.DistanceOp
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell.{Cell, FourGFdd, Micro}
import sa.com.mobily.event.{PsEventSource, Event}
import sa.com.mobily.geometry.{Coordinates, GeomUtils, UtmCoordinates}
import sa.com.mobily.user.User
import sa.com.mobily.utils.{EdmCustomMatchers, LocalSparkContext}

class JourneyTest extends FlatSpec with ShouldMatchers with EdmCustomMatchers with LocalSparkContext {

  trait WithEvents {

    val event1 = Event(User("imei", "imsi", 1L), 0, 0, 1, 1, PsEventSource, Some("EventType"), None, None, None, None)
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

    val cell1 = Cell(1, 1, UtmCoordinates(1, 4), FourGFdd, Micro, 20, 180, 45, 4, "1",
      "POLYGON (( 0 0, 0 4, 2 4, 2 0, 0 0 ))")
    val cell1Centroid = "POINT (1 2)"
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

    val cell50 = cell1.copy(cellId = 50, planarCoords = UtmCoordinates(0, 0),
      coverageWkt = "POLYGON (( 0 0, 0 40, 20 40, 20 0, 0 0 ))")
    val cell51 = cell1.copy(cellId = 51, planarCoords = UtmCoordinates(30, 40),
      coverageWkt = "POLYGON (( 30 40, 70 40, 70 0, 30 0, 30 40 ))")
    val cell52 = cell1.copy(cellId = 52, planarCoords = UtmCoordinates(20, 30),
      coverageWkt = "POLYGON (( 20 30, 20 50, 40 50, 40 30, 20 30 ))")
    val cell53 = cell1.copy(cellId = 53, planarCoords = UtmCoordinates(50, 50),
      coverageWkt = "POLYGON (( 0 60, 0 70, 20 70, 20 60, 0 60 ))")

    implicit val cells =
      Map(
        (1, 1) -> cell1,
        (1, 20) -> cell20,
        (1, 21) -> cell21,
        (1, 22) -> cell22,
        (1, 3) -> cell3,
        (1, 101) -> cell101,
        (1, 120) -> cell120,
        (1, 130) -> cell130,
        (1, 1001) -> cellIntersect1,
        (1, 1002) -> cellIntersect2,
        (1, 1003) -> cellIntersect3,
        (1, 1004) -> cellIntersectWith1And2,
        (1, 1005) -> cellIntersectWith2And3,
        (1, 1006) -> cellContainInitPoint1,
        (1, 50) -> cell50,
        (1, 51) -> cell51,
        (1, 52) -> cell52,
        (1, 53) -> cell53)
  }

  trait WithInitPoints {

    val geomFactory = GeomUtils.geomFactory(Coordinates.SaudiArabiaUtmSrid)
    val initPoint1 = geomFactory.createPoint(new Coordinate(2, 0))
  }

  trait WithSpatioTemporalSlots {

    val origSlot = SpatioTemporalSlot(
      user = User("", "", 1),
      startTime = 1,
      endTime = 10,
      cells = Set((1, 50)),
      firstEventBeginTime = 3,
      lastEventEndTime = 9,
      outMinSpeed = 6,
      intraMinSpeedSum = 0.5,
      numEvents = 2)
    val vp1 = JourneyViaPoint(
      user = User("", "", 1),
      journeyId = 1,
      startTime = 10,
      endTime = 15,
      geomWkt = "POLYGON (( 30 40, 70 40, 70 0, 30 0, 30 40 ))",
      cells = Set((1, 51)),
      firstEventBeginTime = 13,
      lastEventEndTime = 14,
      numEvents = 1)
    val vp2 = vp1.copy(
      startTime = 15,
      endTime = 20,
      geomWkt = "POLYGON (( 20 30, 20 50, 40 50, 40 30, 20 30 ))",
      cells = Set((1, 52)),
      firstEventBeginTime = 17,
      lastEventEndTime = 19,
      numEvents = 1)
    val destSlot = origSlot.copy(
      startTime = 20,
      endTime = 30,
      cells = Set((1, 53)),
      firstEventBeginTime = 24,
      lastEventEndTime = 28,
      outMinSpeed = 1.5,
      numEvents = 3)
    val viaPoints = List(vp1, vp2)
  }

  trait WithJourneys {

    val journey = Journey(
      user = User("", "", 1),
      id = 1,
      startTime = 1,
      endTime = 10,
      geomWkt = "LINESTRING (0.5 0.5, 1 1)",
      cells = Set((1, 1)),
      firstEventBeginTime = 3,
      lastEventEndTime = 8,
      numEvents = 1)
    val builtJourney = Journey(
      user = User("", "", 1),
      id = 0,
      startTime = 10,
      endTime = 20,
      geomWkt = "LINESTRING (10 20, 50 20, 30 40, 10 65)",
      cells = Set((1, 51), (1, 52)),
      firstEventBeginTime = 13,
      lastEventEndTime = 19,
      numEvents = 2)
    val builtJourneyNoVp = builtJourney.copy(
      geomWkt = "LINESTRING (10 20, 10 65)",
      cells = Set(),
      firstEventBeginTime = 10,
      lastEventEndTime = 20,
      numEvents = 0)
  }

  "Journey" should "calculate the seconds in between two events" in new WithEvents {
    Journey.secondsInBetween(event1, event20) should be(5)
  }

  it should "calculate the seconds in between two simultaneous events" in new WithEvents {
    Journey.secondsInBetween(event1, event1) should be(0.001)
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
    Journey.computeMinSpeed(List(event1), cells) should
      be(List(event1.copy(inSpeed = Some(0), outSpeed = Some(0), minSpeedPointWkt = Some(cell1Centroid))))
  }

  it should "compute minimum speed for a list with two events" in new WithCellCatalogue with WithEvents {
    val eventsWithSpeed = Journey.computeMinSpeed(List(event101, event120), cells)
    eventsWithSpeed.map(_.inSpeed) should be(List(Some(0), Some(0.4)))
    eventsWithSpeed.map(_.outSpeed) should be(List(Some(0.4), Some(0)))
    eventsWithSpeed.map(_.minSpeedPointWkt) should be(List(Some("POINT (2 1)"), Some("POINT (2 3)")))
  }

  it should "compute minimum speed for a list with some events belonging to the same cell" in
    new WithCellCatalogue with WithEvents {
      val eventsWithSpeed = Journey.computeMinSpeed(List(event101, event120, event1200, event1201, event130), cells)
      eventsWithSpeed.map(_.inSpeed) should be(List(Some(0.0), Some(0.4), Some(0.0), Some(0.0), Some(1.5)))
      eventsWithSpeed.map(_.outSpeed) should be(List(Some(0.4), Some(0.0), Some(0.0), Some(1.5), Some(0.0)))
      eventsWithSpeed.map(_.minSpeedPointWkt) should
        be(List(
          Some("POINT (2 1)"),
          Some("POINT (2 3)"),
          Some("POINT (2 3)"),
          Some("POINT (2 3)"),
          Some("POINT (2 6)")))
    }

  it should "compute minimum speed for a list with three events (no intersections)" in
    new WithCellCatalogue with WithEvents {
      val eventsWithSpeed = Journey.computeMinSpeed(List(event101, event120, event130), cells)
      eventsWithSpeed.map(_.inSpeed) should be(List(Some(0.0), Some(0.6), Some(0.4)))
      eventsWithSpeed.map(_.outSpeed) should be(List(Some(0.6), Some(0.4), Some(0.0)))
      eventsWithSpeed.map(_.minSpeedPointWkt) should
        be(List(Some("POINT (2 1)"), Some("POINT (2 4)"), Some("POINT (2 6)")))
  }

  it should "compute minimum speed when second intersects third" in new WithCellCatalogue with WithEvents {
    val eventsWithSpeed =
      Journey.computeMinSpeed(List(eventIntersect1, eventIntersectWith2And3, eventIntersect3), cells)
    eventsWithSpeed.map(_.inSpeed) should be(List(Some(0.0), Some(1.375), Some(0.25)))
    eventsWithSpeed.map(_.outSpeed) should be(List(Some(1.375), Some(0.25), Some(0.0)))
    eventsWithSpeed.map(_.minSpeedPointWkt) should
      be(List(Some("POINT (1.5 1.5)"), Some("POINT (7 1.5)"), Some("POINT (8 1.5)")))
  }

  it should "compute minimum speed when first intersects second" in new WithCellCatalogue with WithEvents {
    val eventsWithSpeed =
      Journey.computeMinSpeed(List(eventIntersect1, eventIntersectWith1And2, eventIntersect3), cells)
    eventsWithSpeed.map(_.inSpeed) should be(List(Some(0.0), Some(1.0), Some(0.75)))
    eventsWithSpeed.map(_.outSpeed) should be(List(Some(1.0), Some(0.75), Some(0.0)))
    eventsWithSpeed.map(_.minSpeedPointWkt) should
      be(List(Some("POINT (1.5 1.5)"), Some("POINT (3.5 1.5)"), Some("POINT (8 1.5)")))
  }

  it should "compute minimum speed when first intersects second and second intersects third" in
    new WithCellCatalogue with WithEvents {
      val eventsWithSpeed =
        Journey.computeMinSpeed(List(eventIntersect1, eventIntersectWith1And2, eventIntersect2), cells)
      eventsWithSpeed.map(_.inSpeed) should be(List(Some(0.0), Some(0.75), Some(0.5)))
      eventsWithSpeed.map(_.outSpeed) should be(List(Some(0.75), Some(0.5), Some(0.0)))
      eventsWithSpeed.map(_.minSpeedPointWkt) should
        be(List(Some("POINT (1.5 1.5)"), Some("POINT (3 1.5)"), Some("POINT (4 1.5)")))
    }

  it should "compute minimum speed when initial point is inside second" in new WithCellCatalogue with WithEvents {
    val eventsWithSpeed =
      Journey.computeMinSpeed(List(eventIntersect1, eventContainInitPoint1, eventIntersect2), cells)
    eventsWithSpeed.map(_.inSpeed) should be(List(Some(0.0), Some(0.0), Some(1.25)))
    eventsWithSpeed.map(_.outSpeed) should be(List(Some(0.0), Some(1.25), Some(0.0)))
    eventsWithSpeed.map(_.minSpeedPointWkt) should
      be(List(Some("POINT (1.5 1.5)"), Some("POINT (1.5 1.5)"), Some("POINT (4 1.5)")))
  }

  it should "compute minimum speed when initial point is in the second's border" in
    new WithCellCatalogue with WithEvents {
      val eventsWithSpeed =
        Journey.computeMinSpeed(List(eventIntersect1, eventIntersect2, eventIntersect2, eventIntersect3), cells)
      eventsWithSpeed.map(_.inSpeed) should be(List(Some(0.0), Some(0.625), Some(0.0), Some(1.0)))
      eventsWithSpeed.map(_.outSpeed) should be(List(Some(0.625), Some(0.0), Some(1.0), Some(0.0)))
      eventsWithSpeed.map(_.minSpeedPointWkt) should
        be(List(Some("POINT (1.5 1.5)"), Some("POINT (4 1.5)"), Some("POINT (4 1.5)"), Some("POINT (8 1.5)")))
    }

  it should "build from SpatioTemporal slots and via points" in
    new WithSpatioTemporalSlots with WithJourneys with WithCellCatalogue {
      Journey(orig = origSlot, dest = destSlot, id = 0, viaPoints = viaPoints)(cells) should be (builtJourney)
    }

  it should "build from SpatioTemporal slots without any via points" in
    new WithSpatioTemporalSlots with WithJourneys with WithCellCatalogue {
      Journey(orig = origSlot, dest = destSlot, id = 0, viaPoints = List())(cells) should be (builtJourneyNoVp)
    }

  it should "build geometry from WKT" in new WithJourneys {
    journey.geom should
      equalGeometry(GeomUtils.parseWkt("LINESTRING (0.5 0.5, 1 1)", Coordinates.SaudiArabiaUtmSrid))
  }

  it should "return its fields for printing" in new WithJourneys {
    journey.fields should be (Array("", "", "1", "Unknown", "Unknown", "1", "1970/01/01 03:00:00",
      "1970/01/01 03:00:00", "LINESTRING (0.5 0.5, 1 1)", "(1,1)", "1970/01/01 03:00:00", "1970/01/01 03:00:00", "1",
      "sa"))
  }

  it should "return the proper header" in new WithJourneys {
    Journey.header should be (Array("imei", "imsi", "msisdn", "mcc", "mnc", "id", "startTime", "endTime", "geomWkt",
      "cells", "firstEventBeginTime", "lastEventEndTime", "numEvents", "countryIsoCode"))
  }

  it should "have the same number of elements in fields and header" in new WithJourneys {
    journey.fields.size should be (Journey.header.size)
  }
}
