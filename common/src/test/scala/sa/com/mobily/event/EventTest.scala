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
      source = PsEventSource,
      eventType = Some("859"),
      subsequentLacTac = Some(0),
      subsequentCellId = Some(0))

    val eventWithoutType = event.copy(eventType = None)
    val typeValueNonDefined = "PS.Event.Non Defined"

    val row = Row(Row("866173010386736", "420034122616618", 560917079L),
      1404162126000L, 1404162610000L, 0x052C, 13067, Row(PsEventSource.id), "859", 0, 0, None, None, None)
    val wrongRow = Row(Row(866173010386L, "420034122616618", 560917079L),
      1404162126000L, 1404162610000L, 0x052C, 13067, Row(PsEventSource.id), "859", 0, 0, None, None, None)
    val eventWithMinSpeed = event.copy(inSpeed = Some(0), outSpeed = Some(0), minSpeedPointWkt = Some("POINT (0 0)"))

    val eventHeader = Array("imei", "imsi", "msisdn", "beginTime", "endTime", "lacTac", "cellId", "source", "eventType",
      "subsequentLacTac", "subsequentCellId", "inSpeed", "outSpeed", "minSpeedPointWkt")
    val eventFields = Array("866173010386736", "420034122616618", "560917079", "1404162126000", "1404162610000", "1324",
      "13067", "PS.Event", "859", "0", "0", "", "", "")
  }

  trait WithEventsDifferentSources {

    val eventCsSmsSource = Event(
      User(imei = "866173010386736", imsi = "420034122616618", msisdn = 560917079L),
      beginTime = 1404162126000L,
      endTime = 1404162610000L,
      lacTac = 0x052C,
      cellId = 13067,
      source = CsSmsSource,
      eventType = Some("859"),
      subsequentLacTac = Some(0),
      subsequentCellId = Some(0))

    val rowCsSmsSource = Row(Row("866173010386736", "420034122616618", 560917079L),
      1404162126000L, 1404162610000L, 0x052C, 13067, Row(CsSmsSource.id), "859", 0, 0, None, None, None)

    val eventCsVoiceSource = eventCsSmsSource.copy(source = CsVoiceSource)
    val rowCsVoiceSource = Row(Row("866173010386736", "420034122616618", 560917079L),
      1404162126000L, 1404162610000L, 0x052C, 13067, Row(CsVoiceSource.id), "859", 0, 0, None, None, None)

    val eventCsAInterfaceSource = eventCsSmsSource.copy(source = CsAInterfaceSource)
    val rowCsAInterfaceSource = Row(Row("866173010386736", "420034122616618", 560917079L),
      1404162126000L, 1404162610000L, 0x052C, 13067, Row(CsAInterfaceSource.id), "859", 0, 0, None, None, None)

    val eventCsIuSource = eventCsSmsSource.copy(source = CsIuSource)
    val rowCsIuSource = Row(Row("866173010386736", "420034122616618", 560917079L),
      1404162126000L, 1404162610000L, 0x052C, 13067, Row(CsIuSource.id), "859", 0, 0, None, None, None)

    val eventPsEventSource = eventCsSmsSource.copy(source = PsEventSource)
    val rowPsEventSource = Row(Row("866173010386736", "420034122616618", 560917079L),
      1404162126000L, 1404162610000L, 0x052C, 13067, Row(PsEventSource.id), "859", 0, 0, None, None, None)

    val eventPsUfdrSource = eventCsSmsSource.copy(source = PsUfdrSource)
    val rowPsUfdrSource = Row(Row("866173010386736", "420034122616618", 560917079L),
      1404162126000L, 1404162610000L, 0x052C, 13067, Row(PsUfdrSource.id), "859", 0, 0, None, None, None)
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
    eventWithoutType.typeValue should be (typeValueNonDefined)
  }

  it should "return correct header" in new WithEvents {
    Event.header should be (header)
  }

  it should "return correct fields" in new WithEvents {
    event.fields should be (eventFields)
  }

  it should "parse correctly CsSmsSource" in new WithEvents {
    Event.parseEventSource("CS.SMS") should be (CsSmsSource)
  }

  it should "parse correctly CsVoiceSource" in new WithEvents {
    Event.parseEventSource("CS.Voice") should be (CsVoiceSource)
  }

  it should "parse correctly CsAInterfaceSource" in new WithEvents {
    Event.parseEventSource("Cs.A-Interface") should be (CsAInterfaceSource)
  }

  it should "parse correctly CsIuSource" in new WithEvents {
    Event.parseEventSource("CS.Iu") should be (CsIuSource)
  }

  it should "parse correctly PsEventSource" in new WithEvents {
    Event.parseEventSource("PS.Event") should be (PsEventSource)
  }

  it should "parse correctly PsUfdrSource" in new WithEvents {
    Event.parseEventSource("PS.UFDR") should be (PsUfdrSource)
  }

  it should "be built from Row with a CsSmsSource" in new WithEventsDifferentSources {
    fromRow.fromRow(rowCsSmsSource) should be (eventCsSmsSource)
  }

  it should "be built from Row with a CsVoiceSource" in new WithEventsDifferentSources {
    fromRow.fromRow(rowCsVoiceSource) should be (eventCsVoiceSource)
  }

  it should "be built from Row with a CsAInterfaceSource" in new WithEventsDifferentSources {
    fromRow.fromRow(rowCsAInterfaceSource) should be (eventCsAInterfaceSource)
  }

  it should "be built from Row with a CsIuSource" in new WithEventsDifferentSources {
    fromRow.fromRow(rowCsIuSource) should be (eventCsIuSource)
  }

  it should "be built from Row with a PsEventSource" in new WithEventsDifferentSources {
    fromRow.fromRow(rowPsEventSource) should be (eventPsEventSource)
  }

  it should "be built from Row with a PsUfdrSource" in new WithEventsDifferentSources {
    fromRow.fromRow(rowPsUfdrSource) should be (eventPsUfdrSource)
  }
}
