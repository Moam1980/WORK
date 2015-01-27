/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import org.scalatest.{ShouldMatchers, FlatSpec}

import sa.com.mobily.cell.{Cell, FourGFdd, Micro}
import sa.com.mobily.geometry.{Coordinates, GeomUtils, UtmCoordinates}
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCustomMatchers

class DwellTest extends FlatSpec with ShouldMatchers with EdmCustomMatchers {

  trait WithCellCatalogue {

    val cell1 = Cell(2, 4, UtmCoordinates(1, 4), FourGFdd, Micro, 20, 180, 45, 4, "1",
      "POLYGON ((0 0, 0 20, 20 20, 20 0, 0 0))")
    val cell2 = cell1.copy(cellId = 6, coverageWkt = "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))")

    implicit val cellCatalogue = Map((2, 4) -> cell1, (2, 6) -> cell2)
  }

  trait WithSpatioTemporalSlot {

    val slotWithTwoEvents = SpatioTemporalSlot(
      user = User("", "", 1),
      startTime = 1,
      endTime = 10,
      cells = Set((2, 4), (2, 6)),
      firstEventBeginTime = 3,
      lastEventEndTime = 9,
      outMinSpeed = 0,
      intraMinSpeedSum = 0.5,
      numEvents = 2)
  }

  trait WithDwell {

    val dwell = Dwell(
      user = User("", "", 1),
      startTime = 1,
      endTime = 10,
      geomWkt = "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
      cells = Set((2, 4), (2, 6)),
      firstEventBeginTime = 3,
      lastEventEndTime = 9,
      numEvents = 2)
  }

  "Dwell" should "build from SpatioTemporal slot" in new WithSpatioTemporalSlot with WithDwell with WithCellCatalogue {
    Dwell(slotWithTwoEvents) should be(dwell)
  }

  it should "build geometry from WKT" in new WithDwell {
    dwell.geom should
      equalGeometry(GeomUtils.parseWkt("POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))", Coordinates.SaudiArabiaUtmSrid))
  }

  it should "return its fields for printing" in new WithDwell {
    dwell.fields should be (Array("", "", "1", "Unknown", "Unknown", "1970/01/01 03:00:00", "1970/01/01 03:00:00",
      "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))", "(2,4);(2,6)", "1970/01/01 03:00:00", "1970/01/01 03:00:00", "2",
      "sa"))
  }

  it should "return the proper header" in new WithDwell {
    Dwell.header should be (Array("imei", "imsi", "msisdn", "mcc", "mnc", "startTime", "endTime", "geomWkt", "cells",
      "firstEventBeginTime", "lastEventEndTime", "numEvents", "countryIsoCode"))
  }

  it should "have the same number of elements in fields and header" in new WithDwell {
    dwell.fields.size should be (Dwell.header.size)
  }
}
