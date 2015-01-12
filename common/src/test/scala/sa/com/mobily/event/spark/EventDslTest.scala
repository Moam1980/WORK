/*
 * TODO: License goes here!
 */

package sa.com.mobily.event.spark

import scala.reflect.io.File

import com.github.nscala_time.time.Imports._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell.{Cell, FourGTdd, Macro}
import sa.com.mobily.cell.spark.CellDsl._
import sa.com.mobily.event.{PsEventSource, Event}
import sa.com.mobily.flickering.FlickeringCells
import sa.com.mobily.geometry.UtmCoordinates
import sa.com.mobily.user.User
import sa.com.mobily.utils.{EdmCoreUtils, LocalSparkSqlContext}

class EventDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import EventDsl._

  trait WithPsEventsText {

    val psEventLine1 = "560917079,420034122616618,1,866173010386736,1404162126,1404162610,2,859,100.75.161.156," +
      "173.192.222.168,50101,443,WEB1,84.23.99.177,84.23.99.162,,84.23.99.162,10.210.4.73,1,052C,,330B,,,," +
      "11650,10339,127,110,(null),(null),,,,964,629,"
    val psEventLine2 = "560917079,420034122616618,1,866173010386736"
    val psEventLine3 = "560917079,420034122616618,1,866173010386736,1404162529,1404162578,16,208,100.75.161.156," +
      "17.149.36.144,50098,5223,WEB1,84.23.99.177,84.23.99.162,,84.23.99.162,10.210.4.73,1,052C,,330B,,,," +
      "5320,5332,26,26,(null),(null),,,,87,833,"
    val psEvents = sc.parallelize(List(psEventLine1, psEventLine2, psEventLine3))
  }

  trait WithSmsEventsText {

    val smsEventLine1 = "102203279,307384,2014-01-10T15:26:22.6360000,2014-01-10T15:26:23.3040000,MOB-INTL,,,,," +
      "ANRITSU IUCS,Riyadh Province,,Central,0127560047347220,01275600,420032351263016,,200927432046,200927432046," +
      "447860015021,,,4097,,1409,1409,,984-367,,3G,3G,IUCS,MOS   ,,N131,True,,,,       False,,,10,15,1,1,2,2014,0," +
      "2,,,,,,,,,420,3,,SA,GB,89"
    val smsEventLine2 = "102203279,307384,2014-01-10T15:26:22."
    val smsEventLine3 = "102203280,307384,2014-01-10T15:26:22.6360000,2014-01-10T15:26:23.3040000,MOB-INTL,,,,," +
      "ANRITSU IUCS,Riyadh Province,,Central,0127560047347220,01275600,420032351263016,,200927432046,200927432046," +
      "447860015021,,,4097,,1409,1409,,984-367,,3G,3G,IUCS,MOS   ,,N131,True,,,,       False,,,10,15,1,1,2,2014,0," +
      "2,,,,,,,,,420,3,,SA,GB,89"
    val smsEvents = sc.parallelize(Array(smsEventLine1, smsEventLine2, smsEventLine3))
  }

  trait WithVoiceEventsText {

    val voiceEventLine1 = "102203281,586373,2014-01-10T15:19:18.6520000,2014-01-10T15:19:25.2860000,,6.634," +
      "MOB-MTC,,,M107,,ANRITSU IUCS,,,,0134160098258506,01341600,420034120446252,,966562127836,0,,,,,,,,12566," +
      "12566,1326,1326,,2014-01-10T15:19:25.2860000,,,,,758-944,,,,,,,,442,108,,,3G,3G,IUCS,,,,,,,,MTC   ,False," +
      "False,2014,1,1,2,10,15,108,False,False,0,,,1,,,,1,3.843,2014-01-10T15:19:22.4950000,420,3,SA,,0.294,2.487," +
      "102203281,586373,"
    val voiceEventLine2 = "102203281,586373,2014-01-10T15:19:18."
    val voiceEventLine3 = "102203281,586373,2014-01-10T15:19:18.6520000,2014-01-10T15:19:25.2860000,,6.634," +
      "MOB-MTC,,,M107,,ANRITSU IUCS,,,,0134160098258506,01341600,420034120446252,,966562127836,0,,,,,,,,12566," +
      "12566,1326,1326,,2014-01-10T15:19:25.2860000,,,,,758-944,,,,,,,,442,108,,,3G,3G,IUCS,,,,,,,,MTC   ,False," +
      "False,2014,1,1,2,10,15,108,False,False,0,,,1,,,,1,3.843,2014-01-10T15:19:22.4950000,420,3,SA,,0.294,2.487," +
      "102203281,586373,"
    val voiceEvents = sc.parallelize(Array(voiceEventLine1, voiceEventLine2, voiceEventLine3))
  }

  trait WithEvents {

    val event1 = Event(
      user = User(
        imei = "0134160098258500",
        imsi = "420034120446250",
        msisdn = 560917079L),
      beginTime = 1389363562000L,
      endTime = 1389363565000L,
      lacTac = 1326,
      cellId = 12566,
      source = PsEventSource,
      eventType = Some("1"),
      subsequentLacTac = Some(1326),
      subsequentCellId = Some(12566))
    val event2 = Event(
      user = User(
        imei = "0134160098258501",
        imsi = "420034120446251",
        msisdn = 560917080L),
      beginTime = 1389363561000L,
      endTime = 1389363565000L,
      lacTac = 1326,
      cellId = 12566,
      source = PsEventSource,
      eventType = Some("1"),
      subsequentLacTac = Some(1326),
      subsequentCellId = Some(12566))
    val event3 = Event(
      user = User(
        imei = "0134160098258502",
        imsi = "420034120446252",
        msisdn = 560917079L),
      beginTime = 1389363560000L,
      endTime = 1389363565000L,
      lacTac = 1326,
      cellId = 12566,
      source = PsEventSource,
      eventType = Some("1"),
      subsequentLacTac = Some(1326),
      subsequentCellId = Some(12566))
    val events = sc.parallelize(Array(event1, event2, event3))
  }

  trait WithEventsRows {

    val row =
      Row(Row("866173010386736", "420034122616618", 560917079L),
        1404162126000L, 1404162610000L, 0x052C, 13067, Row(PsEventSource.id), "859", None, None, None, None, None)
    val row2 =
      Row(Row("866173010386735", "420034122616617", 560917080L),
        1404162126001L, 1404162610001L, 0x052C, 13067, Row(PsEventSource.id), "859", None, None, None, None, None)
    val wrongRow =
      Row(Row(866173010386L, "420034122616618", 560917079L),
        1404162126000L, 1404162610000L, 0x052C, 13067, Row(PsEventSource.id), "859", None, None, None, None, None)
    val rows = sc.parallelize(List(row, row2))
    val wrongRows = sc.parallelize(List(row, row2, wrongRow))
  }

  trait WithFlickeringEvents {

    val user1 = User(imei = "0134160098258500", imsi = "420034120446250", msisdn = 560917079L)
    val user2 = User(imei = "0134160098258501", imsi = "420034120446251", msisdn = 560917073L)
    val cell1 = Cell(
      cellId = 4465390,
      lacTac = 57,
      planarCoords = UtmCoordinates(-228902.5, 3490044.0),
      technology = FourGTdd,
      cellType = Macro,
      height = 25.0,
      azimuth = 0.0,
      beamwidth = 216,
      range = 681.54282813,
      bts = "1",
      coverageWkt = "POLYGON ((-228969.5 3490032.3, -228977.7 3490031.6, -229017.2 3490033.9, -229056.8 3490042.1, " +
        "-229095.7 3490056.2, -229132.9 3490076.1, -229167.6 3490101.5, -229199 3490132.2, -229226.2 3490167.5, " +
        "-229248.7 3490206.9, -229265.7 3490249.7, -229277 3490295.1, -229281.9 3490342.2, -229280.5 3490390.1, " +
        "-229272.4 3490438, -229257.9 3490484.8, -229237 3490529.6, -229210.1 3490571.6, -229177.5 3490609.9, " +
        "-229139.9 3490643.7, -229098 3490672.3, -229052.4 3490695.3, -229004 3490712, -228953.7 3490722.1, " +
        "-228902.5 3490725.5, -228851.3 3490722.1, -228801 3490712, -228752.6 3490695.3, -228707 3490672.3, " +
        "-228665.1 3490643.7, -228627.5 3490609.9, -228594.9 3490571.6, -228568 3490529.6, -228547.1 3490484.8, " +
        "-228532.6 3490438, -228524.5 3490390.1, -228523.1 3490342.2, -228528 3490295.1, -228539.3 3490249.7, " +
        "-228556.3 3490206.9, -228578.8 3490167.5, -228606 3490132.2, -228637.4 3490101.5, -228672.1 3490076.1, " +
        "-228709.3 3490056.2, -228748.2 3490042.1, -228787.8 3490033.9, -228827.3 3490031.6, -228835.5 3490032.3, " +
        "-228836.5 3490027.1, -228839.1 3490018.9, -228842.8 3490011.2, -228847.4 3490003.9, -228852.8 3489997.3, " +
        "-228859.1 3489991.5, -228866 3489986.5, -228873.5 3489982.3, -228881.4 3489979.2, -228889.7 3489977.1, " +
        "-228898.2 3489976, -228906.8 3489976, -228915.3 3489977.1, -228923.6 3489979.2, -228931.5 3489982.3, " +
        "-228939 3489986.5, -228945.9 3489991.5, -228952.2 3489997.3, -228957.6 3490003.9, -228962.2 3490011.2, " +
        "-228965.9 3490018.9, -228968.5 3490027.1, -228969.5 3490032.3))",
      mcc = "420",
      mnc = "03")
    val cell2 = cell1.copy(cellId = 4465391)
    val event1 = Event(
      user1,
      beginTime = 1389363562000L,
      endTime = 1389363565000L,
      lacTac = cell1.lacTac,
      cellId = cell1.cellId,
      source = PsEventSource,
      eventType = Some("1"),
      subsequentLacTac = Some(1326),
      subsequentCellId = Some(12566))
    val event2 = event1.copy(beginTime = 1389363563000L)
    val event3 = event1.copy(beginTime = 1389363562001L, lacTac = cell2.lacTac, cellId = cell2.cellId)
    val event4 = event3.copy(user = user2)
    val events = sc.parallelize(Array(event1, event2, event3, event4))
    val cells = sc.parallelize(Array(cell1, cell2))
    val flickeringCells = Array(FlickeringCells(Set((cell1.lacTac , cell1.cellId), (cell2.lacTac , cell2.cellId))))
    val broadcastCellCatalogue = cells.toBroadcastMap
  }

  trait WithMatchingCells {

    val user1 = User(imei = "0134160098258500", imsi = "420034120446250", msisdn = 560917079L)
    val cell1 = Cell(
      cellId = 4465390,
      lacTac = 51,
      planarCoords = UtmCoordinates(-228902.5, 3490044.0),
      technology = FourGTdd,
      cellType = Macro,
      height = 25.0,
      azimuth = 0.0,
      beamwidth = 216,
      range = 681.54282813,
      bts = "1",
      coverageWkt = "POLYGON ((0 0, 1 1, 2 2, 0 2, 0 0))",
      mcc = "420",
      mnc = "03")
    val cell2 = cell1.copy(lacTac = 52)
    val cell3 = cell1.copy(lacTac = 53)
    val event1 = Event(
      user1,
      beginTime = 1389363562000L,
      endTime = 1389363565000L,
      lacTac = cell1.lacTac,
      cellId = cell1.cellId,
      source = PsEventSource,
      eventType = Some("1"),
      subsequentLacTac = Some(1326),
      subsequentCellId = Some(12566))
    val event2 = event1.copy(lacTac = cell2.lacTac, cellId = cell2.cellId)
    val event3 = event1.copy(lacTac = cell3.lacTac, cellId = cell3.cellId)
    val broadcastCellCatalogue = sc.parallelize(Array(cell1, cell2)).toBroadcastMap
    val events = sc.parallelize(Array(event1, event2, event3))
  }

  trait WithWeekEvents {

    val cell = Cell(
      cellId = 4465390,
      lacTac = 51,
      planarCoords = UtmCoordinates(-228902.5, 3490044.0),
      technology = FourGTdd,
      cellType = Macro,
      height = 25.0,
      azimuth = 0.0,
      beamwidth = 216,
      range = 681.54282813,
      bts = "1",
      coverageWkt = "POLYGON ((0 0, 1 1, 2 2, 0 2, 0 0))",
      mcc = "420",
      mnc = "03")
    val beginTime1 =
      DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140818")
    val beginTime2 =
      DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140824")
    val beginTime3 =
      DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140819")
    val beginTime4 =
      DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140825")

    val event1 = Event(
      user = User(imei = "0134160098258500", imsi = "420034120446250", msisdn = 560917079L),
      beginTime = beginTime1.hourOfDay.setCopy(0).getMillis,
      endTime = beginTime1.hourOfDay.setCopy(2).getMillis,
      lacTac = cell.lacTac,
      cellId = cell.cellId,
      source = PsEventSource,
      eventType = Some("1"),
      subsequentLacTac = Some(1326),
      subsequentCellId = Some(12566))
    val event2 = event1.copy(
      beginTime = beginTime2.hourOfDay.setCopy(0).getMillis,
      endTime = beginTime2.hourOfDay.setCopy(0).getMillis)
    val event3 = event1.copy(
      beginTime = beginTime2.hourOfDay.setCopy(23).getMillis,
      endTime = beginTime2.hourOfDay.setCopy(23).getMillis)
    val event4 = event1.copy(
      beginTime = beginTime3.hourOfDay.setCopy(1).getMillis,
      endTime = beginTime3.hourOfDay.setCopy(1).getMillis)
    val event5 = event1.copy(
      beginTime = beginTime3.hourOfDay.setCopy(23).getMillis,
      endTime = beginTime3.hourOfDay.setCopy(23).getMillis)
    val event6 = event1.copy(
      user = User("", "", 1L),
      beginTime = beginTime4.hourOfDay.setCopy(1).getMillis,
      endTime = beginTime4.hourOfDay.setCopy(3).getMillis)

    val events = sc.parallelize(List(event1, event2, event3, event4, event5, event6))
    val cellCatalogue = sc.parallelize(List(cell)).toBroadcastMap
    val user560917079VectorResult = Vectors.sparse(
      Event.HoursInWeek,
      Seq((0, 1.0), (23, 1.0), (24, 1.0), (25, 1.0), (26, 1.0), (49, 1.0), (71, 1.0)))
    val user1VectorResult = Vectors.sparse(Event.HoursInWeek, Seq((25, 1.0), (26, 1.0), (27, 1.0)))
  }

  trait WithWeekEventsLittleActivity extends WithWeekEvents {
    
    val event7 = event1.copy(
      beginTime = beginTime2.hourOfDay.setCopy(0).minuteOfHour.setCopy(1).getMillis,
      endTime = beginTime2.hourOfDay.setCopy(7).minuteOfHour.setCopy(2).getMillis)
    val event8 = event1.copy(
      beginTime = beginTime1.hourOfDay.setCopy(0).minuteOfHour.setCopy(1).getMillis,
      endTime = beginTime1.hourOfDay.setCopy(7).minuteOfHour.setCopy(2).getMillis)
    val eventsLowActivity = sc.parallelize(List(event7, event8, event4, event5, event6))
  }

  trait WithEventsByCell extends WithMatchingCells {

    val group1 = (cell3.identifier, Iterable(event3))
    val group2 = (cell1.identifier, Iterable(event1))
    val group3 = (cell2.identifier, Iterable(event2))
    val eventsByCell = List(group1, group2, group3)
  }

  trait WithUsersByCell extends WithMatchingCells {

    val user4 = User(imei = "0434160098258500", imsi = "42034120446250", msisdn = 3809175479L)
    val event4 = event1.copy(user = user4)
    val event5 = event1.copy(user = event2.user)
    val event6 = event1.copy(user = user4)
    override val events = sc.parallelize(List(event1, event2, event3, event4, event5, event6))
    val group1 = (cell3.identifier, Iterable(event3.user))
    val group2 = (cell1.identifier, Iterable(user1, user4))
    val group3 = (cell2.identifier, Iterable(event2.user))
    val usersByCell = List(group1, group2, group3)
    val countUsersByCell = List((cell3.identifier, 1), (cell1.identifier, 4), (cell2.identifier, 1))

    val userGroup1 = (cell3.identifier, Map(event3.user -> 1))
    val userGroup2 = (cell1.identifier, Map(user1 -> 2, user4 -> 2))
    val userGroup3 = (cell2.identifier, Map(event2.user -> 1))
    val eventsByCellAndUser = List(userGroup1, userGroup2, userGroup3)
  }

  "EventDsl" should "get correctly parsed PS events" in new WithPsEventsText {
    psEvents.psToEvent.count should be (2)
  }

  it should "get errors when parsing PS events" in new WithPsEventsText {
    psEvents.psToEventErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed PS events" in new WithPsEventsText {
    psEvents.psToParsedEvent.count should be (3)
  }

  it should "get correctly parsed SMS events" in new WithSmsEventsText {
    smsEvents.smsToEvent.count should be (2)
  }

  it should "get errors when parsing SMS events" in new WithSmsEventsText {
    smsEvents.smsToEventErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed SMS events" in new WithSmsEventsText {
    smsEvents.smsToParsedEvent.count should be (3)
  }

  it should "get correctly parsed voice events" in new WithVoiceEventsText {
    voiceEvents.voiceToEvent.count should be (2)
  }

  it should "get errors when parsing voice events" in new WithVoiceEventsText {
    voiceEvents.voiceToEventErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed voice events" in new WithVoiceEventsText {
    voiceEvents.voiceToParsedEvent.count should be (3)
  }

  it should "group by user chronologically" in new WithEvents {
    val orderedEvents = events.byUserChronologically.collect.toMap
    orderedEvents.size should be (2)
    orderedEvents(560917079L) should be (List(event3, event1))
    orderedEvents(560917080L) should be (List(event2))
  }

  it should "get correctly parsed rows" in new WithEventsRows {
    rows.toEvent.count should be (2)
  }

  it should "save events in parquet" in new WithEvents {
    val path = File.makeTemp().name
    events.saveAsParquetFile(path)
    sqc.parquetFile(path).toEvent.collect.sameElements(events.collect) should be (true)
    File(path).deleteRecursively
  }

  it should "detect flickering for user Events" in new WithFlickeringEvents {
    val analysis = events.flickeringDetector(2000)(broadcastCellCatalogue).collect
    analysis.length should be (1)
    analysis should be (flickeringCells)
  }

  it should "filter events with cells matching in the cell catalogue" in new WithMatchingCells  {
    val eventsWithMatchingCell = events.withMatchingCell(broadcastCellCatalogue).collect
    eventsWithMatchingCell.length should be (2)
    eventsWithMatchingCell should contain (event1)
    eventsWithMatchingCell should contain (event2)
  }

  it should "filter events with cells not matching in the cell catalogue" in new WithMatchingCells  {
    val eventsWithNonMatchingCell = events.withNonMatchingCell(broadcastCellCatalogue).collect
    eventsWithNonMatchingCell.length should be (1)
    eventsWithNonMatchingCell should contain (event3)
  }

  it should "save as CSV metrics of events" in new WithEvents {
    val path = File.makeTemp().name
    events.saveMetrics(path)
    events.sparkContext.textFile(path).count should be (5)
    File(path).deleteRecursively
  }
  it should "calculate the vector to home clustering correctly" in new WithWeekEvents {
    val homes = events.toUserActivity(cellCatalogue).collect
    homes.length should be (2)
    homes.find(_.user.msisdn == 560917079).get.activityVector should be (user560917079VectorResult)
    homes.find(_.user.msisdn == 1).get.activityVector should be (user1VectorResult)
  }

  it should "calculate the vector to home clustering filtering little activity users" in
    new WithWeekEventsLittleActivity {
      val homes = eventsLowActivity.perUserAndSiteIdFilteringLittleActivity()(cellCatalogue).collect
      homes.length should be (1)
    }

  it should "calculate the vector to home clustering filtering little activity users and overriding default ratio" in
    new WithWeekEventsLittleActivity {
      val homes = eventsLowActivity.perUserAndSiteIdFilteringLittleActivity(0.2)(cellCatalogue).collect
      homes.length should be (0)
    }

  it should "group events by cell" in new WithEventsByCell {
    events.toEventsByCell.collect.toList should be (eventsByCell)
  }

  it should "count users by cell" in new WithUsersByCell {
    events.countUsersByCell.collect.toList should be (countUsersByCell)
  }

  it should "count events grouping it by users and cell" in new WithUsersByCell {
    events.toEventsByCellAndUser.collect.toList should be (eventsByCellAndUser)
  }
}
