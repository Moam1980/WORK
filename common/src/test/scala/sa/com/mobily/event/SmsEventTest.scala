/*
 * TODO: License goes here!
 */

package sa.com.mobily.event

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.parsing.CsvParser
import sa.com.mobily.user.User

class SmsEventTest extends FlatSpec with ShouldMatchers {

  import SmsEvent._

  trait WithSmsEvent {

    val smsEventLine = "102203279,307384,2014-01-10T15:26:22.6360000,2014-01-10T15:26:23.3040000,MOB-INTL,,,,," +
      "ANRITSU IUCS, Riyadh Province,,Central,0127560047347220,01275600,420032351263016,,200927432046,200927432046," +
      "447860015021,,,4097,,1409,1409,,984-367,,3G,3G,IUCS,MOS   ,,N131,True,,,,False,,,10,15,1,1,2,2014,0," +
      "2,,,,,,,,,420,3,,SA,GB,89"
    val smsEventFields: Array[String] = Array("102203279", "307384", "2014-01-10T15:26:22.6360000",
      "2014-01-10T15:26:23.3040000", "MOB-INTL", "", "", "", "", "ANRITSU IUCS", "Riyadh Province", "", "Central",
      "0127560047347220", "01275600", "420032351263016", "", "200927432046", "200927432046", "447860015021", "", "",
      "4097", "", "1409", "1409", "", "984-367", "", "3G", "3G", "IUCS", "MOS   ", "", "N131", "True", "", "", "",
      "False", "", "", "10", "15", "1", "1", "2", "2014", "0", "2", "", "", "", "", "", "", "", "", "420", "3", "",
      "SA", "GB", "89")
    val event = Event(
      user = User(
        imei = "0127560047347220",
        imsi = "420032351263016",
        msisdn = 200927432046L),
      beginTime = 1389356782636L,
      endTime = 1389356783304L,
      lacTac = 1409,
      cellId = 4097,
      eventType = "2",
      subsequentLacTac = Some(1409),
      subsequentCellId = None)
  }

  "SmsEvent" should "be built from CSV" in new WithSmsEvent {
    CsvParser.fromLine(smsEventLine).value.get should be (event)
  }

  it should "be discarded when CSV format is wrong" in new WithSmsEvent {
    an [Exception] should be thrownBy fromCsv.fromFields(smsEventFields.updated(2, "A"))
  }
}
