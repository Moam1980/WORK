/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric.spark

import scala.reflect.io.File

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell.{Micro, FourGFdd, Cell}
import sa.com.mobily.event.{PsEventSource, Event}
import sa.com.mobily.geometry.UtmCoordinates
import sa.com.mobily.user.User
import sa.com.mobily.usercentric._
import sa.com.mobily.utils.LocalSparkSqlContext

class UserModelDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import UserModelDsl._

  trait WithEvents {

    val event1 = Event(
      User(imei = "1", imsi = "1", msisdn = 1),
      beginTime = 1,
      endTime = 2,
      lacTac = 1,
      cellId = 2,
      source = PsEventSource,
      eventType = Some("859"),
      subsequentLacTac = Some(0),
      subsequentCellId = Some(0),
      inSpeed = Some(0),
      outSpeed = Some(3),
      minSpeedPointWkt = Some("POINT (1 1)"))
    val event2 = event1.copy(beginTime = 3, endTime = 4, cellId = 3, inSpeed = Some(3))
    val event3 = event1.copy(beginTime = 5, endTime = 6, cellId = 5, inSpeed = Some(3))
    val event4 = event1.copy(beginTime = 1, endTime = 3, cellId = 6, inSpeed = Some(3))
    val event5 = event1.copy(beginTime = 8, endTime = 10, cellId = 7)
    val events = sc.parallelize(Array(event1, event2, event3, event5))
    val withSameCellEvents = sc.parallelize(Array(event1, event1, event2, event3, event3, event3, event4, event5))
  }

  trait WithCellCatalogue {

    val cellPrefixWkt = "POLYGON ((0 20, 10 20, 10 30, 0 30, 0 20))"
    val cellSlot1Wkt = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))"
    val cellSlot2Wkt = "POLYGON ((5 0, 15 0, 15 10, 5 10, 5 0))"
    val cellSuffixWkt = "POLYGON ((20 0, 20 10, 30 10, 30 0, 20 0))"
    val slot1AfterWkt = "POLYGON ((200 200, 210 200, 210 210, 200 210, 200 200))"
    val slot2AfterWkt = "POLYGON ((205 200, 215 200, 215 210, 205 210, 205 200))"
    val suffix2Wkt = "POLYGON ((220 200, 220 210, 230 210, 230 200, 220 200))"
    val cellTimeExt1Wkt = "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))"
    val cellTimeExt3Wkt = "POLYGON ((100010 0, 100010 10, 100020 10, 100020 0, 100010 0))"
    val cellTimeExt4Wkt = "POLYGON ((200900 0, 200900 10, 200910 10, 200910 0, 200900 0))"

    val cellPrefix = Cell(1, 1, UtmCoordinates(1, 4), FourGFdd, Micro, 20, 180, 45, 4, "1", cellPrefixWkt)
    val cellSlot1 = cellPrefix.copy(cellId = 2, coverageWkt = cellSlot1Wkt)
    val cellSlot2 = cellPrefix.copy(cellId = 3, coverageWkt = cellSlot2Wkt)
    val cellSuffix = cellPrefix.copy(cellId = 4, coverageWkt = cellSuffixWkt)
    val cellSlot1After = cellPrefix.copy(cellId = 5, coverageWkt = slot1AfterWkt)
    val cellSlot2After = cellPrefix.copy(cellId = 6, coverageWkt = slot2AfterWkt)
    val cellSuffix2 = cellPrefix.copy(cellId = 7, coverageWkt = suffix2Wkt)
    val cellTimeExt1 = Cell(1, 2, UtmCoordinates(0, 0), FourGFdd, Micro, 20, 180, 45, 4, "1", cellTimeExt1Wkt)
    val cellTimeExt3 =
      cellTimeExt1.copy(cellId = 3, planarCoords = UtmCoordinates(100010, 0), coverageWkt = cellTimeExt3Wkt)
    val cellTimeExt4 =
      cellTimeExt1.copy(cellId = 4, planarCoords = UtmCoordinates(200900, 0), coverageWkt = cellTimeExt4Wkt)


    implicit val bcCellCatalogue = sc.parallelize(
      Array(cellPrefix, cellSlot1, cellSlot2, cellSuffix, cellSlot1After, cellSlot2After, cellSuffix2, cellTimeExt1,
        cellTimeExt3, cellTimeExt4)).toBroadcastMap
  }

  trait WithSpatioTemporalSlots extends WithEvents {

    val slot1 = SpatioTemporalSlot(
      user = User("", "", 1),
      startTime = 1,
      endTime = 2,
      cells = Set((1, 1)),
      firstEventBeginTime = 1,
      lastEventEndTime = 2,
      outMinSpeed = 0.5,
      intraMinSpeedSum = 0,
      numEvents = 1)
    val slot2 = SpatioTemporalSlot(
      user = User("", "", 1),
      startTime = 2,
      endTime = 5,
      cells = Set((1, 2)),
      firstEventBeginTime = 2,
      lastEventEndTime = 5,
      outMinSpeed = 0,
      intraMinSpeedSum = 0,
      numEvents = 1)
    val slot1WithScore = slot1.copy(score = Some(CompatibilityScore(1, 0, 0.5)))
  }

  trait WithCompatibilitySlots extends WithEvents {

    val prefixSlot = SpatioTemporalSlot(
      user = User("", "", 1),
      startTime = 0,
      endTime = 10,
      cells = Set((1, 1)),
      firstEventBeginTime = 0,
      lastEventEndTime = 10,
      outMinSpeed = 0,
      intraMinSpeedSum = 0,
      numEvents = 1,
      score = Some(CompatibilityScore(0, 0, 0)))
    val slot1 = prefixSlot.copy(
      startTime = 10,
      endTime = 20,
      cells = Set((1, 2)),
      firstEventBeginTime = 10,
      lastEventEndTime = 20,
      score = Some(CompatibilityScore(0.5, 0, 0)))
    val slot2 = prefixSlot.copy(
      startTime = 20,
      endTime = 30,
      cells = Set((1, 3)),
      firstEventBeginTime = 20,
      lastEventEndTime = 30,
      score = Some(CompatibilityScore(0, 0, 0)))
    val suffixSlot = prefixSlot.copy(
      startTime = 30,
      endTime = 40,
      cells = Set((1, 4)),
      firstEventBeginTime = 30,
      lastEventEndTime = 40,
      score = None)

    val mergedSlot = slot1.copy(
      endTime = 30,
      cells = Set((1, 2), (1, 3)),
      lastEventEndTime = 30,
      outMinSpeed = 0,
      intraMinSpeedSum = 0,
      numEvents = 2,
      score = Some(CompatibilityScore(0, 0, 0)))

    val slot1After = prefixSlot.copy(
      startTime = 40,
      endTime = 50,
      cells = Set((1, 5)),
      firstEventBeginTime = 40,
      lastEventEndTime = 50,
      score = Some(CompatibilityScore(0.5, 0, 0)))
    val slot2After = prefixSlot.copy(
      startTime = 50,
      endTime = 60,
      cells = Set((1, 6)),
      firstEventBeginTime = 50,
      lastEventEndTime = 60,
      score = Some(CompatibilityScore(0, 0, 0)))
    val suffixSlot2 = prefixSlot.copy(
      startTime = 60,
      endTime = 70,
      cells = Set((1, 7)),
      firstEventBeginTime = 60,
      lastEventEndTime = 70,
      score = None)

    val merged2Slot = slot1After.copy(
      endTime = 60,
      cells = Set((1, 5), (1, 6)),
      lastEventEndTime = 60,
      outMinSpeed = 0,
      intraMinSpeedSum = 0,
      numEvents = 2,
      score = Some(CompatibilityScore(0, 0, 0)))

    val slots = sc.parallelize(
      Array((User("", "", 1L), List(prefixSlot, slot1, slot2, suffixSlot, slot1After, slot2After, suffixSlot2))))
    val onlySlot = sc.parallelize(Array((User("", "", 1L), List(prefixSlot))))
  }

  trait WithModelSlotsForTimeExtension {

    val startOfDay = 1423429200000L
    val endOfDay = 1423515599999L

    val dwellSlot1 = SpatioTemporalSlot(
      user = User("", "1", 1),
      startTime = 1423468095000L,
      endTime = 1423469636000L,
      cells = Set((2, 1)),
      firstEventBeginTime = 1423468095000L,
      lastEventEndTime = 1423469636000L,
      outMinSpeed = 7.5,
      intraMinSpeedSum = 0.5,
      numEvents = 4)
    val viaPoint16 = SpatioTemporalSlot(
      user = User("", "1", 1),
      startTime = 1423489415000L,
      endTime = 1423489931000L,
      cells = Set((2, 3)),
      firstEventBeginTime = 1423489415000L,
      lastEventEndTime = 1423489931000L,
      outMinSpeed = 7.5,
      intraMinSpeedSum = 0.5,
      numEvents = 4,
      typeEstimate = JourneyViaPointEstimate)
    val dwellSlot31 = SpatioTemporalSlot(
      user = User("", "1", 1),
      startTime = 1423507833000L,
      endTime = 1423514411000L,
      cells = Set((2, 4)),
      firstEventBeginTime = 1423507833000L,
      lastEventEndTime = 1423514411000L,
      outMinSpeed = 0.5,
      intraMinSpeedSum = 0.5,
      numEvents = 4)

    val slotsForTimeExtension = sc.parallelize(Array((User("", "1", 1), List(dwellSlot1, viaPoint16, dwellSlot31))))
  }

  trait WithModelSlots extends WithCellCatalogue {

    val slot1 = SpatioTemporalSlot(
      user = User("", "", 1),
      startTime = 1000000,
      endTime = 2000000,
      cells = Set((1, 1)),
      firstEventBeginTime = 1000000,
      lastEventEndTime = 2000000,
      outMinSpeed = 7.5,
      intraMinSpeedSum = 0.5,
      numEvents = 3)
    val slotJvp1 = slot1.copy(
      startTime = 2000000,
      endTime = 3000000,
      cells = Set((1, 2)),
      firstEventBeginTime = 2000000,
      lastEventEndTime = 3000000,
      outMinSpeed = 10)
    val slotJvp2 = slot1.copy(
      startTime = 5000000,
      endTime = 7000000,
      cells = Set((1, 3)),
      firstEventBeginTime = 5000000,
      lastEventEndTime = 7000000,
      outMinSpeed = 8)
    val slot2 = slot1.copy(
      startTime = 10000000,
      endTime = 15000000,
      cells = Set((1, 4)),
      firstEventBeginTime = 10000000,
      lastEventEndTime = 15000000,
      outMinSpeed = 0)
    val slot3 = slot1.copy(
      startTime = 30000000,
      endTime = 35000000,
      cells = Set((1, 5)),
      firstEventBeginTime = 30000000,
      lastEventEndTime = 35000000,
      outMinSpeed = 0)

    val slots = sc.parallelize(Array((User("", "", 1L), List(slot1, slotJvp1, slotJvp2, slot2, slot3))))
  }

  trait WithUserModelText {

    val dwellLine1 = "|420032153783846|0|1970/01/01 03:00:00|1970/01/01 03:00:01|" +
      "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))|(2,4);(2,6)|1970/01/01 03:00:00|1970/01/01 03:00:01|4|sa"
    val dwellLine2 = "|420032153783846|0|Not Valid!|1970/01/01 03:00:01|" +
      "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))|(2,4);(2,6)|1970/01/01 03:00:00|1970/01/01 03:00:01|4|sa"
    val dwellLine3 = "|3|0|1970/01/01 03:00:00|1970/01/01 03:00:01|" +
      "POLYGON ((0 0, 0 30, 30 30, 30 0, 0 0))|(2,4);(2,6)|1970/01/01 03:00:00|1970/01/01 03:00:01|4|sa"

    val journeyLine1 = "|420032153783846|0|0|1970/01/01 03:00:00|1970/01/01 03:00:01|" +
      "LINESTRING (258620.1 2031643.7, 256667.6 2035865.5)||1970/01/01 03:00:00|1970/01/01 03:00:01|3|sa"
    val journeyLine2 = "|420032153783846|0|0|1970/01/01 03:00:00|1970/01/01 03:00:01|" +
      "LINESTRING (258620.1 2031643.7, 256667.6 2035865.5)||Not Valid!|1970/01/01 03:00:01|3|sa"
    val journeyLine3 = "|3|0|0|1970/01/01 03:00:00|1970/01/01 03:00:01|" +
      "LINESTRING (258620.1 2031643.7, 256667.6 2035865.5)||1970/01/01 03:00:00|1970/01/01 03:00:01|3|sa"

    val jvpLine1 = "|420032181160624|0|0|1970/01/01 03:00:00|1970/01/01 03:00:01|" +
      "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))|(1202,12751)|1970/01/01 03:00:00|1970/01/01 03:00:01|1|sa"
    val jvpLine2 = "|420032181160624|0|0|1970/01/01 03:00:00|1970/01/01 03:00:01|" +
      "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))|(1202,12751)|Not Valid !|1970/01/01 03:00:01|1|sa"
    val jvpLine3 = "|18|0|0|1970/01/01 03:00:00|1970/01/01 03:00:01|" +
      "POLYGON ((0 0, 0 80, 80 80, 80 0, 0 0))|(1202,12751)|1970/01/01 03:00:00|1970/01/01 03:00:01|1|sa"

    val dwells = sc.parallelize(List(dwellLine1, dwellLine2, dwellLine3))
    val journeys = sc.parallelize(List(journeyLine1, journeyLine2, journeyLine3))
    val jvps = sc.parallelize(List(jvpLine1, jvpLine2, jvpLine3))
  }

  trait WithUserModelRows {

    val dwellRow1 =
      Row(Row("", "420032153783846", 0L), 0L, 1000L, "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
        Row(Row(2, 4), Row(2, 6)), 0L, 1000L, 4L, "sa")
    val dwellRow2 =
      Row(Row("", "420032153783846", 0L), 0L, 1000L, "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
        Row(), 0L, 1000L, 4L, "sa")
    val dwellRows = sc.parallelize(List(dwellRow1, dwellRow2))
    val dwell1 = Dwell(
      user = User("", "420032153783846", 0),
      startTime = 0,
      endTime = 1000,
      geomWkt = "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
      cells = Seq((2, 4), (2, 6)),
      firstEventBeginTime = 0,
      lastEventEndTime = 1000,
      numEvents = 4)
    val dwell2 = dwell1.copy(cells = Seq())
    val dwells = sc.parallelize(List(dwell1, dwell2))

    val journeyRow1 = Row(Row("", "420032153783846", 0L), 0, 0L, 1000L,
      "LINESTRING (258620.1 2031643.7, 256667.6 2035865.5)", Row(Row(2, 4), Row(2, 6)), 0L, 1000L, 3L, "sa")
    val journeyRow2 = Row(Row("", "420032153783846", 0L), 0, 0L, 1000L,
      "LINESTRING (258620.1 2031643.7, 256667.6 2035865.5)", Row(), 0L, 1000L, 3L, "sa")
    val journeyRows = sc.parallelize(List(journeyRow1, journeyRow2))
    val journey1 = Journey(
      user = User("", "420032153783846", 0),
      id = 0,
      startTime = 0,
      endTime = 1000,
      geomWkt = "LINESTRING (258620.1 2031643.7, 256667.6 2035865.5)",
      cells = Seq((2, 4), (2, 6)),
      firstEventBeginTime = 0,
      lastEventEndTime = 1000,
      numEvents = 3)
    val journey2 = journey1.copy(cells = Seq())
    val journeys = sc.parallelize(List(journey1, journey2))

    val journeyVpRow1 = Row(Row("", "420032181160624", 0L), 0, 0L, 1000L, "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
      Row(Row(1202, 12751)), 0L, 1000L, 1L, "sa")
    val journeyVpRow2 = Row(Row("", "420032181160624", 0L), 0, 0L, 1000L, "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
      Row(), 0L, 1000L, 1L, "sa")
    val journeyViaPointRows = sc.parallelize(List(journeyVpRow1, journeyVpRow2))
    val journeyVp1 = JourneyViaPoint(
      user = User("", "420032181160624", 0),
      journeyId = 0,
      startTime = 0,
      endTime = 1000,
      geomWkt = "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
      cells = Seq((1202, 12751)),
      firstEventBeginTime = 0,
      lastEventEndTime = 1000,
      numEvents = 1)
    val journeyVp2 = journeyVp1.copy(cells = Seq())
    val journeyViaPoints = sc.parallelize(List(journeyVp1, journeyVp2))
  }

  trait WithDwells {

    val user1 = User("", "4200301", 0)
    val user2 = User("", "4200302", 0)

    val dwell1 = Dwell(
      user = user1,
      startTime = 3600000,
      endTime = 7200000,
      geomWkt = "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))",
      cells = Seq((2, 4), (2, 6)),
      firstEventBeginTime = 3,
      lastEventEndTime = 9,
      numEvents = 2)
    val dwell2 = dwell1.copy(startTime = 7400000, endTime = 8100000)
    val dwell3 = dwell1.copy(user = user2)

    val dwells = sc.parallelize(Array(dwell3, dwell2, dwell1))
    val usersForFiltering = sc.parallelize(Array(user1))
  }

  trait WithDwellsStatistics extends WithDwells {

    val dwell4 = dwell1.copy(user = dwell1.user.copy("", "1234567", 0))
    val dwell5 = dwell1.copy(
      startTime = 1414875600000L,
      endTime = 1414875900000L,
      geomWkt = "POLYGON ((0 0, 0 20, 20 20, 20 0, 0 0))")
    val dwell6 = dwell1.copy(startTime = 1414875600000L, endTime = 1414875900000L)

    val dwellsStatistics = sc.parallelize(Array(dwell1, dwell4, dwell5, dwell6))
    val stats = dwellsStatistics.toDwellStats
  }

  trait WithModelSlotsForViaPointDetection {

    val dwellSlot1 = SpatioTemporalSlot(
      user = User("", "1", 1),
      startTime = 0L,
      endTime = 1000L,
      cells = Set((1, 2)),
      firstEventBeginTime = 0L,
      lastEventEndTime = 50L,
      outMinSpeed = 0.5,
      intraMinSpeedSum = 0.5,
      numEvents = 4)
    val dwellSlot2 = dwellSlot1.copy(startTime = 1000L, endTime = 301000L, cells = Set((1, 4)))
    val dwellSlot3 = dwellSlot1.copy(startTime = 1000000L, endTime = 1900000L, cells = Set((1, 5)))
    val dwellSlot4 = dwellSlot1.copy(startTime = 1905000L, endTime = 1995000L, cells = Set((1, 7)))
    val dwellSlot5 = dwellSlot1.copy(startTime = 2000000L, endTime = 2200000L, cells = Set((2, 3)))

    val slots =
      sc.parallelize(Array((dwellSlot1.user, List(dwellSlot1, dwellSlot2, dwellSlot3, dwellSlot4, dwellSlot5))))
  }

  "UserModelDsl" should "not aggregate when there are no consecutive events having the same cell " +
    "(or overlapping in time)" in new WithSpatioTemporalSlots with WithCellCatalogue {
      events.byUserChronologically.aggTemporalOverlapAndSameCell.first._2.size should be (4)
    }

  it should "aggregate consecutive events having the same cell (or overlapping in time)" in
    new WithSpatioTemporalSlots with WithCellCatalogue {
      withSameCellEvents.byUserChronologically.aggTemporalOverlapAndSameCell.first._2.size should be(4)
    }

  it should "merge compatible slots" in new WithCompatibilitySlots with WithCellCatalogue {
    slots.combine.first._2.size should be(5)
  }

  it should "merge no slots when there is only one" in new WithCompatibilitySlots with WithCellCatalogue {
    onlySlot.combine.first._2.size should be(1)
  }

  it should "create user model entities" in new WithModelSlots {
    val model = slots.toUserCentric.first._2
    model._1.size should be (3)
    model._2.size should be (2)
    model._3.size should be (2)
  }

  it should "extend time of user model entities" in new WithModelSlotsForTimeExtension with WithCellCatalogue {
    val modelEntities = slotsForTimeExtension.toUserCentric.first._2
    modelEntities._1.size should be (2)
    val dwell1 = modelEntities._1.head
    val dwell2 = modelEntities._1.last
    dwell1.startTime should be (startOfDay)
    dwell1.endTime should be (viaPoint16.startTime - 4000000)
    dwell2.startTime should be (viaPoint16.endTime + 4036000)
    dwell2.endTime should be (endOfDay)
    modelEntities._3.size should be (1)
    val viaPoint = modelEntities._3.head
    viaPoint.startTime should be (viaPoint16.startTime)
    viaPoint.endTime should be (viaPoint16.endTime)
  }

  it should "get correctly parsed dwells" in new WithUserModelText {
    dwells.toDwell.count should be (2)
  }

  it should "get errors when parsing dwells" in new WithUserModelText {
    dwells.toDwellErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed dwells" in new WithUserModelText {
    dwells.toParsedDwell.count should be (3)
  }

  it should "get correctly parsed journeys" in new WithUserModelText {
    journeys.toJourney.count should be (2)
  }

  it should "get errors when parsing journeys" in new WithUserModelText {
    journeys.toJourneyErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed journeys" in new WithUserModelText {
    journeys.toParsedJourney.count should be (3)
  }

  it should "get correctly parsed journey via points" in new WithUserModelText {
    jvps.toJourneyViaPoint.count should be (2)
  }

  it should "get errors when parsing journey via points" in new WithUserModelText {
    jvps.toJourneyViaPointErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed journey via points" in new WithUserModelText {
    jvps.toParsedJourneyViaPoint.count should be (3)
  }

  it should "get correctly parsed dwell rows" in new WithUserModelRows {
    dwellRows.toDwell.count should be (2)
  }

  it should "save dwells in parquet" in new WithUserModelRows {
    val path = File.makeTemp().name
    dwells.saveAsParquetFile(path)
    sqc.parquetFile(path).toDwell.collect.sameElements(dwells.collect) should be (true)
    File(path).deleteRecursively
  }

  it should "get correctly parsed journey rows" in new WithUserModelRows {
    journeyRows.toJourney.count should be (2)
  }

  it should "save journeys in parquet" in new WithUserModelRows {
    val path = File.makeTemp().name
    journeys.saveAsParquetFile(path)
    sqc.parquetFile(path).toJourney.collect.sameElements(journeys.collect) should be (true)
    File(path).deleteRecursively
  }

  it should "get correctly parsed journey via point rows" in new WithUserModelRows {
    journeyViaPointRows.toJourneyViaPoint.count should be (2)
  }

  it should "save journey via points in parquet" in new WithUserModelRows {
    val path = File.makeTemp().name
    journeyViaPoints.saveAsParquetFile(path)
    sqc.parquetFile(path).toJourneyViaPoint.collect.sameElements(journeyViaPoints.collect) should be (true)
    File(path).deleteRecursively
  }

  it should "group dwells by user chronologically" in new WithDwells {
    val perUser = dwells.byUserChronologically.collect.toMap
    perUser.size should be (2)
    perUser(User("", "4200301", 0)) should be (List(dwell1, dwell2))
    perUser(User("", "4200302", 0)) should be (List(dwell3))
  }

  it should "get user model stats for number of dwells" in new WithDwellsStatistics {
    val numberOfDwellsStats = stats.filter(s => s.metricType == TotalNumberOfDwells).collect
    numberOfDwellsStats.size should be(2)
    numberOfDwellsStats.find(_.date == dwell1.formattedDay).get.count should be(2)
    numberOfDwellsStats.find(_.date == dwell5.formattedDay).get.count should be(2)
  }

  it should "get user model stats for user detected by dwells" in new WithDwellsStatistics {
    val userDetectedByDwellsStats = stats.filter(s => s.metricType == TotalNumberOfUsersByDwells).collect
    userDetectedByDwellsStats.size should be(2)
    userDetectedByDwellsStats.find(_.date == dwell1.formattedDay).get.count should be(2)
    userDetectedByDwellsStats.find(_.date == dwell5.formattedDay).get.count should be(1)
  }

  it should "get user model stats for dwells mean" in new WithDwellsStatistics {
    val meanDwellsByUserStats = stats.filter(s => s.metricType == MeanDwellsByUser).collect
    meanDwellsByUserStats.size should be(2)
    meanDwellsByUserStats.find(_.date == dwell1.formattedDay).get.count should be(1.0)
    meanDwellsByUserStats.find(_.date == dwell5.formattedDay).get.count should be(2.0)
  }

  it should "get user model stats for dwells standard deviation" in new WithDwellsStatistics {
    val stdDeviationDwellsByUserStats = stats.filter(s => s.metricType == StdDeviationDwellsByUser).collect
    stdDeviationDwellsByUserStats.size should be(2)
    stdDeviationDwellsByUserStats.find(_.date == dwell1.formattedDay).get.count should be(0.0)
    stdDeviationDwellsByUserStats.find(_.date == dwell5.formattedDay).get.count should be(0.0)
  }

  it should "get user model stats for dwells area mean" in new WithDwellsStatistics {
    val meanDwellsAreaStats = stats.filter(s => s.metricType == MeanDwellsArea).collect
    meanDwellsAreaStats.size should be(2)
    meanDwellsAreaStats.find(_.date == dwell1.formattedDay).get.count should be(100.0)
    meanDwellsAreaStats.find(_.date == dwell5.formattedDay).get.count should be(250.0)
  }

  it should "get user model stats for dwells area standard deviation" in new WithDwellsStatistics {
    val stDeviationDwellsAreaStats = stats.filter(s => s.metricType == StdDeviationDwellsArea).collect
    stDeviationDwellsAreaStats.size should be(2)
    stDeviationDwellsAreaStats.find(_.date == dwell1.formattedDay).get.count should be(0.0)
    stDeviationDwellsAreaStats.find(_.date == dwell5.formattedDay).get.count should be(150.0)
  }

  it should "get user model stats for dwells duration mean" in new WithDwellsStatistics {
    val meanDwellsDurationStats = stats.filter(s => s.metricType == MeanDwellsDuration).collect
    meanDwellsDurationStats.size should be(2)
    meanDwellsDurationStats.find(_.date == dwell1.formattedDay).get.count should be(60.0)
    meanDwellsDurationStats.find(_.date == dwell5.formattedDay).get.count should be(5.0)
  }

  it should "get user model stats for dwells duration standard deviation" in new WithDwellsStatistics {
    val stDeviationDwellsDurationStats = stats.filter(s => s.metricType == StdDeviationDwellsDuration).collect
    stDeviationDwellsDurationStats.size should be(2)
    stDeviationDwellsDurationStats.find(_.date == dwell1.formattedDay).get.count should be(0.0)
    stDeviationDwellsDurationStats.find(_.date == dwell5.formattedDay).get.count should be(0.0)
  }

  it should "convert suitable dwell slots to via points when building the model" in
    new WithModelSlotsForViaPointDetection with WithCellCatalogue {
      val model = slots.toUserCentric.first._2
      model._1.size should be (3)
      model._2.size should be (2)
      model._3.size should be (2)
    }

  it should "get dwells for certain users" in new WithDwells {
    dwells.forUsers(usersForFiltering).collect should contain theSameElementsAs(List(dwell1, dwell2))
  }
}
