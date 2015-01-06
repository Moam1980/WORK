/*
 * TODO: License goes here!
 */

package sa.com.mobily.event

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.parsing.CsvParser
import sa.com.mobily.user.User

class PsEventTest extends FlatSpec with ShouldMatchers {

  import PsEvent._

  trait WithPsEvent {

    val psEventLine = "560917079,420034122616618,1,866173010386736,1404162126,1404162610,2,859,100.75.161.156," +
      "173.192.222.168,50101,443,WEB1,84.23.99.177,84.23.99.162,,84.23.99.162,10.210.4.73,1,052C,,330B,,,," +
      "11650,10339,127,110,(null),(null),,,,964,629,"
    val psEventFields: Array[String] = Array("560917079", "420034122616618", "1", "866173010386736", "1404162126",
      "1404162610", "2", "859", "100.75.161.156", "173.192.222.168", "50101", "443", "WEB1", "84.23.99.177",
      "84.23.99.162", "", "84.23.99.162", "10.210.4.73", "1", "052C", "", "330B", "", "", "", "11650", "10339",
      "127", "110", "(null)", "(null)", "", "", "", "964", "629", "")
    val event = Event(
      user = User(
        imei = "866173010386736",
        imsi = "420034122616618",
      msisdn = 560917079L),
      beginTime = 1404162126000L,
      endTime = 1404162610000L,
      lacTac = 0x052C,
      cellId = 13067,
      eventType = Some("859"),
      subsequentLacTac = None,
      subsequentCellId = None)
  }

  "PsEvent" should "be built from CSV" in new WithPsEvent {
    CsvParser.fromLine(psEventLine).value.get should be (event)
  }

  it should "be discarded when CSV format is wrong" in new WithPsEvent {
    an [Exception] should be thrownBy fromCsv.fromFields(psEventFields.updated(4, "A"))
  }
}
