/*
 * TODO: License goes here!
 */

package sa.com.mobily.event

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.parsing.CsvParser
import sa.com.mobily.user.User

class VoiceEventTest extends FlatSpec with ShouldMatchers {

  import VoiceEvent._

  trait WithVoiceEvent {

    val voiceEventLine = "102203281,586373,2014-01-10T15:19:18.6520000,2014-01-10T15:19:25.2860000,,6.634,MOB-MTC,,," +
        "M107,,ANRITSU IUCS,,,,0134160098258506,01341600,420034120446252,,966562127836,560917079,,,,,,,,12566,12566,1326," +
        "1326,,2014-01-10T15:19:25.2860000,,,,,758-944,,,,,,,,442,108,,,3G,3G,IUCS,,,,,,,,MTC   ,False,False," +
        "2014,1,1,2,10,15,108,False,False,0,,,1,,,,1,3.843,2014-01-10T15:19:22.4950000,420,3,SA,,0.294,2.487," +
        "102203281,586373,"
    val voiceEventFields: Array[String] = Array("102203281", "586373", "2014-01-10T15:19:18.6520000",
      "2014-01-10T15:19:25.2860000", "", "6.634", "MOB-MTC", "", "", "M107", "", "ANRITSU IUCS", "", "", "",
      "0134160098258506", "01341600", "420034120446252", "", "966562127836", "0", "", "", "", "", "", "", "", "12566",
      "12566", "1326", "1326", "", "2014-01-10T15:19:25.2860000", "", "", "", "", "758-944", "", "", "", "", "", "",
      "", "442", "108", "", "", "3G", "3G", "IUCS", "", "", "", "", "", "", "", "MTC   ", "False", "False", "2014",
      "1", "1", "2", "10", "15", "108", "False", "False", "0", "", "", "1", "", "", "", "1", "3.843",
      "2014-01-10T15:19:22.4950000", "420", "3", "SA", "", "0.294", "2.487", "102203281", "586373", "")
    val event = Event(
      user = User(
        imei = "0134160098258506",
        imsi = "420034120446252",
        msisdn = 560917079L),
      beginTime = 1389356358652L,
      endTime = 1389356365286L,
      lacTac = 1326,
      cellId = 12566,
      eventType = Some("1"),
      subsequentLacTac = Some(1326),
      subsequentCellId = Some(12566))
  }

  "VoiceEvent" should "be built from CSV" in new WithVoiceEvent {
    CsvParser.fromLine(voiceEventLine).value.get should be (event)
  }

  it should "be discarded when CSV format is wrong" in new WithVoiceEvent {
    an [Exception] should be thrownBy fromCsv.fromFields(voiceEventFields.updated(2, "A"))
  }
}
