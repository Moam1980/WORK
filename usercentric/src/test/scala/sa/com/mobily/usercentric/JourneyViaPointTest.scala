/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest._

import sa.com.mobily.cell.{Cell, FourGFdd, Micro}
import sa.com.mobily.geometry.{Coordinates, GeomUtils, UtmCoordinates}
import sa.com.mobily.parsing.CsvParser
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCustomMatchers

class JourneyViaPointTest extends FlatSpec with ShouldMatchers with EdmCustomMatchers {

  import JourneyViaPoint._

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

    val journeyVpLine = "|420032181160624|0|0|1970/01/01 03:00:00|1970/01/01 03:00:01|" +
      "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))|(1202,12751)|1970/01/01 03:00:00|1970/01/01 03:00:01|1|sa"
    val journeyVpFields = Array("", "420032181160624", "0", "0", "1970/01/01 03:00:00", "1970/01/01 03:00:01",
      "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))", "(1202,12751)", "1970/01/01 03:00:00", "1970/01/01 03:00:01", "1",
      "sa")
    val jvpRow = Row(Row("", "420032181160624", 0L), 0, 0L, 1000L, "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
      Row(Row(1202, 12751)), 0L, 1000L, 1L, "sa")
    val jvpRowNoCells = Row(Row("", "420032181160624", 0L), 0, 0L, 1000L, "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
      Row(), 0L, 1000L, 1L, "sa")
    val jvpWrongRow = Row(Row("", "420032181160624", 0L), 0, 0L, 1000L, "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
      Row("3"), 0L, 1000L, 1L, "sa")

    val journeyVpRead = JourneyViaPoint(
      user = User("", "420032181160624", 0),
      journeyId = 0,
      startTime = 0,
      endTime = 1000,
      geomWkt = "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
      cells = Seq((1202, 12751)),
      firstEventBeginTime = 0,
      lastEventEndTime = 1000,
      numEvents = 1)

    val journeyVp = JourneyViaPoint(
      user = User("", "", 1),
      journeyId = 3,
      startTime = 3000,
      endTime = 8000,
      geomWkt = "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
      cells = Seq((1, 1)),
      firstEventBeginTime = 3000,
      lastEventEndTime = 8000,
      numEvents = 1)
  }

  "JourneyViaPoint" should "build from via point slot" in
    new WithCellCatalogue with WithSpatioTemporalSlot with WithJourneyViaPoint {
      JourneyViaPoint(viaPointSlot, 3) should be (journeyVp)
    }

  it should "build geometry from WKT" in new WithJourneyViaPoint {
    journeyVp.geom should
      equalGeometry(GeomUtils.parseWkt("POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))", Coordinates.SaudiArabiaUtmSrid))
  }

  it should "return its fields for printing" in new WithJourneyViaPoint {
    journeyVp.fields should be (Array("", "", "1", "3", "1970/01/01 03:00:03", "1970/01/01 03:00:08",
      "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))", "(1,1)", "1970/01/01 03:00:03", "1970/01/01 03:00:08", "1", "sa"))
  }

  it should "return the proper header" in new WithJourneyViaPoint {
    JourneyViaPoint.Header should be (Array("imei", "imsi", "msisdn", "journeyId", "startTime", "endTime", "geomWkt",
      "cells", "firstEventBeginTime", "lastEventEndTime", "numEvents", "countryIsoCode"))
  }

  it should "have the same number of elements in fields and header" in new WithJourneyViaPoint {
    journeyVp.fields.size should be (JourneyViaPoint.Header.size)
  }

  it should "be built from CSV" in new WithJourneyViaPoint {
    CsvParser.fromLine(journeyVpLine).value.get should be (journeyVpRead)
  }

  it should "be discarded when the CSV format is wrong" in new WithJourneyViaPoint {
    an [Exception] should be thrownBy fromCsv.fromFields(journeyVpFields.updated(4, "NotValidTime"))
  }

  it should "be built from Row (with cells)" in new WithJourneyViaPoint {
    fromRow.fromRow(jvpRow) should be (journeyVpRead)
  }

  it should "be built from Row (with no cells)" in new WithJourneyViaPoint {
    fromRow.fromRow(jvpRowNoCells) should be (journeyVpRead.copy(cells = Seq()))
  }

  it should "be discarded when row is wrong" in new WithJourneyViaPoint {
    an[Exception] should be thrownBy fromRow.fromRow(jvpWrongRow)
  }
}
