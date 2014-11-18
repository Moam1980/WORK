/*
 * TODO: License goes here!
 */

package sa.com.mobily.event.spark

import scala.reflect.io.File

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.event.Event
import sa.com.mobily.user.User
import sa.com.mobily.utils.LocalSparkSqlContext

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
      eventType = "1",
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
      eventType = "1",
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
      eventType = "1",
      subsequentLacTac = Some(1326),
      subsequentCellId = Some(12566))
    val events = sc.parallelize(Array(event1, event2, event3))
  }

  trait WithEventsRows {

    val row =
      Row(Row("866173010386736", "420034122616618", 560917079L),
        1404162126000L, 1404162610000L, 0x052C, 13067, "859", None, None, None, None)
    val row2 =
      Row(Row("866173010386735", "420034122616617", 560917080L),
        1404162126001L, 1404162610001L, 0x052C, 13067, "859", None, None, None, None)
    val wrongRow =
      Row(Row(866173010386L, "420034122616618", 560917079L),
        1404162126000L, 1404162610000L, 0x052C, 13067, "859", None, None, None, None)
    val rows = sc.parallelize(List(row, row2))
    val wrongRows = sc.parallelize(List(row, row2, wrongRow))
  }

  trait WithFlickeringEvents {

    val user1 = User(imei = "0134160098258500", imsi = "420034120446250", msisdn = 560917079L)
    val user2 = User(imei = "0134160098258501", imsi = "420034120446251", msisdn = 560917073L)
    val cell1 = (1326, 12566)
    val cell2 = (1325, 1212565566)
    val event1 = Event(
      user1,
      beginTime = 1389363562000L,
      endTime = 1389363565000L,
      lacTac = cell1._1,
      cellId = cell1._2,
      eventType = "1",
      subsequentLacTac = Some(1326),
      subsequentCellId = Some(12566))
    val event2 = event1.copy(beginTime = 1389363563000L)
    val event3 = event1.copy(beginTime = 1389363562001L, lacTac = cell2._1, cellId = cell2._2)
    val event4 = event3.copy(user = user2)
    val events = sc.parallelize(Array(event1, event2, event3, event4))
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
    val analysis = events.flickeringAnalysis(2000).collect
    analysis.length should be (1)
    analysis should be (Seq(Set(cell1, cell2)))
  }
}
