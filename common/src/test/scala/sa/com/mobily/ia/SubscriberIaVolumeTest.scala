/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.scalatest.{FlatSpec, ShouldMatchers}
import sa.com.mobily.parsing.CsvParser


class SubscriberIaVolumeTest extends FlatSpec with ShouldMatchers {

  import SubscriberIaVolume._

  trait WithSubscriberIaVolume {

    val subscriberVolumeLine = "\"12556247\"|\"18\""
    val fields = Array("12556247", "18")

    val subscriberVolume = SubscriberIaVolume(subscriberId = "12556247", volumeBytes = 18D)
  }

  "SubscriberIaVolume" should "be built from CSV" in new WithSubscriberIaVolume {
    CsvParser.fromLine(subscriberVolumeLine).value.get should be (subscriberVolume)
  }

  it should "be discarded when the CSV format is wrong" in new WithSubscriberIaVolume {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(2, "WrongNumber"))
  }
}
