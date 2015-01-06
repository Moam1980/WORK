/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric.spark

import org.scalatest.{ShouldMatchers, FlatSpec}

import sa.com.mobily.cell.{Micro, FourGFdd, Cell}
import sa.com.mobily.cell.spark.CellDsl._
import sa.com.mobily.event.Event
import sa.com.mobily.event.spark.EventDsl._
import sa.com.mobily.geometry.UtmCoordinates
import sa.com.mobily.user.User
import sa.com.mobily.utils.LocalSparkSqlContext

class JourneyDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import JourneyDsl._

  trait WithEvents {

    val event101 = Event(User("imei", "imsi", 1L), 0, 0, 1, 101, Some("EventType"), None, None, None, None)
    val eventWithNoCell = event101.copy(cellId = 999)
    val event120 = event101.copy(beginTime = 5000, endTime = 5000, cellId = 120)
    val event130 = event101.copy(beginTime = 10000, endTime = 10000, cellId = 130)

    val eventsAllJoiningCell = sc.parallelize(Array(event101, event120, event130))
    val eventsSomeNotJoiningCell = sc.parallelize(Array(event101, eventWithNoCell, event120, event130))

    val event101WithMinSpeed =
      event101.copy(inSpeed = Some(0.0), outSpeed = Some(0.6), minSpeedPointWkt = Some("POINT (2 1)"))
    val event120WithMinSpeed =
      event120.copy(inSpeed = Some(0.6), outSpeed = Some(0.4), minSpeedPointWkt = Some("POINT (2 4)"))
    val event130WithMinSpeed =
      event130.copy(inSpeed = Some(0.4), outSpeed = Some(0.0), minSpeedPointWkt = Some("POINT (2 6)"))
  }

  trait WithCellCatalogue {

    val cell101 = Cell(101, 1, UtmCoordinates(1, 5), FourGFdd, Micro, 20, 180, 45, 4, "1",
      "POLYGON (( 1 0, 1 2, 3 2, 3 0, 1 0 ))")
    val cell120 = cell101.copy(cellId = 120, planarCoords = UtmCoordinates(1, 5),
      coverageWkt = "POLYGON (( 2 3, 2 5, 4 5, 4 3, 2 3 ))")
    val cell130 = cell101.copy(cellId = 130, planarCoords = UtmCoordinates(1, 5),
      coverageWkt = "POLYGON (( 0 6, 0 7, 2 7, 2 6, 0 6 ))")

    val cells = sc.parallelize(List(cell101, cell120, cell130)).toBroadcastMap
  }

  trait WithJourneyVisualisationText {

    val event101To120Text = "LINESTRING (2 1, 2 4)|1970/01/01|03:00:00-03:00:05 (2.2 km/h)|" +
      "POLYGON (( 1 0, 1 2, 3 2, 3 0, 1 0 ))|POLYGON (( 2 3, 2 5, 4 5, 4 3, 2 3 ))"
    val event120To130Text = "LINESTRING (2 4, 2 6)|1970/01/01|03:00:05-03:00:10 (1.4 km/h)|" +
      "POLYGON (( 2 3, 2 5, 4 5, 4 3, 2 3 ))|POLYGON (( 0 6, 0 7, 2 7, 2 6, 0 6 ))"
  }

  "JourneyDsl" should "compute shortest paths and minimum speeds (and their points path)" in
    new WithCellCatalogue with WithEvents {
      val withMinSpeeds = eventsAllJoiningCell.byUserChronologically.withMinSpeeds(cells)
      withMinSpeeds.count should be (1)
      withMinSpeeds.first should be ((1L, List(event101WithMinSpeed, event120WithMinSpeed, event130WithMinSpeed)))
    }

  it should "generate shortest path segments and event geometries for visualisation" in
    new WithCellCatalogue with WithEvents with WithJourneyVisualisationText {
      val withSegmentsAndGeometries =
        eventsAllJoiningCell.byUserChronologically.withMinSpeeds(cells).segmentsAndGeometries(cells)
      withSegmentsAndGeometries.count should be (1)
      withSegmentsAndGeometries.first should be ((1L, List(event101To120Text, event120To130Text)))
    }
}
