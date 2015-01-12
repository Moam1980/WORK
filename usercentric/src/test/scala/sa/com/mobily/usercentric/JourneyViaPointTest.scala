/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import org.scalatest._

import sa.com.mobily.cell.{Cell, FourGFdd, Micro}
import sa.com.mobily.geometry.{Coordinates, GeomUtils, UtmCoordinates}
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCustomMatchers

class JourneyViaPointTest extends FlatSpec with ShouldMatchers with EdmCustomMatchers {

  trait WithCellCatalogue {

    val cell1 = Cell(1, 1, UtmCoordinates(1, 4), FourGFdd, Micro, 20, 180, 45, 4, "1",
      "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))")
    implicit val cells = Map((1, 1) -> cell1)
  }

  trait WithSpatioTemporalSlot {

    val viaPointSlot = SpatioTemporalSlot(
      user = User("", "", 1),
      startTime = 3000,
      endTime = 8000,
      cells = Set((1, 1)),
      firstEventBeginTime = 3000,
      lastEventEndTime = 8000,
      outMinSpeed = 6,
      intraMinSpeedSum = 0.5,
      numEvents = 1,
      typeEstimate = JourneyViaPointEstimate)
  }

  trait WithJourneyViaPoint {

    val journeyVp = JourneyViaPoint(
      user = User("", "", 1),
      journeyId = 3,
      startTime = 3000,
      endTime = 8000,
      geomWkt = "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
      cells = Set((1, 1)),
      firstEventBeginTime = 3000,
      lastEventEndTime = 8000,
      numEvents = 1)
  }

  "JourneyViaPoint" should "" in new WithCellCatalogue with WithSpatioTemporalSlot with WithJourneyViaPoint {
    JourneyViaPoint(viaPointSlot, 3) should be (journeyVp)
  }

  it should "build geometry from WKT" in new WithJourneyViaPoint {
    journeyVp.geom should
      equalGeometry(GeomUtils.parseWkt("POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))", Coordinates.SaudiArabiaUtmSrid))
  }

  it should "return its fields for printing" in new WithJourneyViaPoint {
    journeyVp.fields should be (Array("", "", "1", "Unknown", "Unknown", "3", "1970/01/01 03:00:03",
      "1970/01/01 03:00:08", "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))", "(1,1)", "1970/01/01 03:00:03",
      "1970/01/01 03:00:08", "1", "sa"))
  }

  it should "return the proper header" in new WithJourneyViaPoint {
    JourneyViaPoint.header should be (Array("imei", "imsi", "msisdn", "mcc", "mnc", "journeyId", "startTime",
      "endTime", "geomWkt", "cells", "firstEventBeginTime", "lastEventEndTime", "numEvents", "countryIsoCode"))
  }

  it should "have the same number of elements in fields and header" in new WithJourneyViaPoint {
    journeyVp.fields.size should be (JourneyViaPoint.header.size)
  }
}
