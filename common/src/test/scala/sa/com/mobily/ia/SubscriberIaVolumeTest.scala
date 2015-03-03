/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.parsing.CsvParser

class SubscriberIaVolumeTest extends FlatSpec with ShouldMatchers {

  import SubscriberIaVolume._

  trait WithSubscriberIaVolume {

    val subscriberVolumeLine = "\"12556247\"|\"18\""
    val fields = Array("12556247", "18")

    val subscriberIaVolumeFields = Array("12556247", "18.0")
    val subscriberIaVolumeHeader = Array("subscriberId", "volumeBytes")

    val row = Row("12556247", 18D)
    val wrongRow = Row("12556247", "NaN")

    val subscriberIaVolume = SubscriberIaVolume(subscriberId = "12556247", volumeBytes = 18D)
  }

  "SubscriberIaVolume" should "return correct header" in new WithSubscriberIaVolume {
    SubscriberIaVolume.Header should be (subscriberIaVolumeHeader)
  }

  it should "return correct fields" in new WithSubscriberIaVolume {
    subscriberIaVolume.fields should be (subscriberIaVolumeFields)
  }

  it should "have same number of elements fields and header" in new WithSubscriberIaVolume {
    subscriberIaVolume.fields.length should be (SubscriberIaVolume.Header.length)
  }

  it should "be built from CSV" in new WithSubscriberIaVolume {
    CsvParser.fromLine(subscriberVolumeLine).value.get should be (subscriberIaVolume)
  }

  it should "be discarded when the CSV format is wrong" in new WithSubscriberIaVolume {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(2, "WrongNumber"))
  }

  it should "be built from Row" in new WithSubscriberIaVolume {
    fromRow.fromRow(row) should be (subscriberIaVolume)
  }

  it should "be discarded when row is wrong" in new WithSubscriberIaVolume {
    an[Exception] should be thrownBy fromRow.fromRow(wrongRow)
  }
}
