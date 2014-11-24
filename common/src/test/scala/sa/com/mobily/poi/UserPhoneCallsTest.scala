/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import com.github.nscala_time.time.Imports._
import org.scalatest._

import sa.com.mobily.parsing.CsvParser
import sa.com.mobily.utils.EdmCoreUtils

class UserPhoneCallsTest extends FlatSpec with ShouldMatchers {

  import UserPhoneCalls._

  trait WhitPhoneCallText {
    val phoneCallText = "'0500001413','20140824',2541,'1','01, 02',"
    val fields = Array("'0500001413'","'20140824'","2541","'1'","'01, 02',")
    val phoneCallsObjetct = UserPhoneCalls(500001413, DateTimeFormat.forPattern("yyyymmdd").
      withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140824"), "2541", 1, Seq(1, 2))
  }

  "UserPhoneCalls" should "be built from CSV" in new WhitPhoneCallText {
    CsvParser.fromLine(phoneCallText).value.get should be (phoneCallsObjetct)
  }

  it should "be discarded when the CSV format is wrong" in new WhitPhoneCallText {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(2, "WrongRegionId"))
  }
}
