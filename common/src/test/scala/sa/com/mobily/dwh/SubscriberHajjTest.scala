/*
 * TODO: License goes here!
 */

package sa.com.mobily.dwh

import org.scalatest.{FlatSpec, ShouldMatchers}
import sa.com.mobily.parsing.CsvParser


class SubscriberHajjTest extends FlatSpec with ShouldMatchers {

  import SubscriberHajj._

  trait WithSubscriberHajj {

    val timestamp = 364683600000L
    val inputDateFormat = "yyyyMMdd"

    val subscriberDwhLine = "200540000000,N/A,20AUG_10OCT_14,Y,Saudi Arabia,N,Y,0.000,INTERNAL,Consumer,Prepaid," +
      "Connect 5G Pre,Connect,TENURE_GRT90 = \"Y\" and MAKKAH_MADINAH_L3M = \"N\",INTERNAL"
    val fields = Array("200540000000", "N/A", "20AUG_10OCT_14", "Y", "Saudi Arabia", "N", "Y", "0.000", "INTERNAL",
      "Consumer", "Prepaid", "Connect 5G Pre,Connect,TENURE_GRT90 = \"Y\" and MAKKAH_MADINAH_L3M = \"N\"", "INTERNAL")

    val subscriberDwh = SubscriberHajj(200540000000L, "N/A", "20AUG_10OCT_14", Some(true), "Saudi Arabia", Some(false),
      Some(true), 0D, "INTERNAL", "Consumer", "Prepaid", "Connect 5G Pre", "Connect",
      "TENURE_GRT90 = \"Y\" and MAKKAH_MADINAH_L3M = \"N\"", "INTERNAL")
  }

  "SubscriberHajjDWH" should "be built from CSV" in new WithSubscriberHajj {
    CsvParser.fromLine(subscriberDwhLine).value.get should be (subscriberDwh)
  }

  it should "be discarded when the CSV format is wrong" in new WithSubscriberHajj {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(0, "WrongNumber"))
  }
}
