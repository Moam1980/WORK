/*
 * TODO: License goes here!
 */

package sa.com.mobily.event

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell.{FourGFdd, Cell, Micro}
import sa.com.mobily.cell.spark.CellDsl._
import sa.com.mobily.geometry.UtmCoordinates
import sa.com.mobily.user.User
import sa.com.mobily.utils.{EdmCustomMatchers, LocalSparkContext}

class EventTest extends FlatSpec with ShouldMatchers with LocalSparkContext with EdmCustomMatchers {

  import Event._

  trait WithEvents {

    val event = Event(
      User(imei = "866173010386736", imsi = "420034122616618", msisdn = 560917079L),
      beginTime = 1404162126000L,
      endTime = 1404162610000L,
      lacTac = 0x052C,
      cellId = 13067,
      eventType = Some("859"),
      subsequentLacTac = Some(0),
      subsequentCellId = Some(0))

    val eventWithoutType = event.copy(eventType = None)
    val typeNonDefined = "Non Defined"

    val row = Row(Row("866173010386736", "420034122616618", 560917079L),
      1404162126000L, 1404162610000L, 0x052C, 13067, "859", 0, 0, None, None, None)
    val wrongRow = Row(Row(866173010386L, "420034122616618", 560917079L),
      1404162126000L, 1404162610000L, 0x052C, 13067, "859", 0, 0, None, None, None)
    val eventWithMinSpeed = event.copy(inSpeed = Some(0), outSpeed = Some(0), minSpeedPointWkt = Some("POINT (0 0)"))
  }

  trait WithCellCatalogue {

    val cell1 = Cell(13067, 1324, UtmCoordinates(1, 4), FourGFdd, Micro, 20, 180, 45, 4, "1",
      "POLYGON (( 0 0, 0 4, 2 4, 2 0, 0 0 ))")
    val cells = sc.parallelize(List(cell1)).toBroadcastMap.value
  }

  "Event" should "prefer LAC to TAC" in {
    Event.lacOrTac("1", "2") should be ("1")
  }

  it should "return TAC when LAC is missing" in {
    Event.lacOrTac("", "2") should be ("2")
  }

  it should "prefer SAC to CI" in {
    Event.sacOrCi("1", "2") should be ("1")
  }

  it should "return CI when SAC is missing" in {
    Event.sacOrCi("", "2") should be ("2")
  }

  it should "be built from Row with a Event" in new WithEvents {
    fromRow.fromRow(row) should be (event)
  }

  it should "be discarded when row is wrong" in new WithEvents {
    an[Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }

  it should "identify events without minimum speed and shortest path point defined" in new WithEvents {
    event.minSpeedPopulated should be (false)
  }

  it should "identify events with minimum speed and shortest path point defined" in new WithEvents {
    eventWithMinSpeed.minSpeedPopulated should be (true)
  }

  it should "provide with cell geometry for an event" in new WithEvents with WithCellCatalogue {
    Event.geom(cells)(event) should equalGeometry(cell1.coverageGeom)
  }

  it should "provide with cell geometry WKT for an event" in new WithEvents with WithCellCatalogue {
    Event.geomWkt(cells)(event) should be (cell1.coverageWkt)
  }

  it should "return correct event type when is not defined" in new WithEvents {
    eventWithoutType.typeValue should be (typeNonDefined)
  }
}
