/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.reflect.io.File

import org.apache.spark.sql.catalyst.expressions.Row
import org.scalatest._

import sa.com.mobily.ia.SubscriberIaVolume
import sa.com.mobily.utils.LocalSparkSqlContext

class SubscriberIaVolumeDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import SubscriberIaVolumeDsl._

  trait WithSubscriberIaVolumeText {

    val subscriberVolume1 = "\"12556247\"|\"18\""
    val subscriberVolume2 = "\"12556247\"|\"444\""
    val subscriberVolume3 = "\"12556247\"|\"NAN\""

    val subscriberVolume = sc.parallelize(List(subscriberVolume1, subscriberVolume2, subscriberVolume3))
  }

  trait WithSubscriberIaVolumeRows {

    val row = Row("12556247", 18D)
    val row2 = Row("22222222", 22.22D)
    val wrongRow = Row("12556247", "NaN")

    val rows = sc.parallelize(List(row, row2))
  }

  trait WithSubscriberIaVolume {

    val subscriberIaVolume1 = SubscriberIaVolume(subscriberId = "12556247", volumeBytes = 18D)
    val subscriberIaVolume2 = SubscriberIaVolume(subscriberId = "22222222", volumeBytes = 22.22D)
    val subscriberIaVolume3 = SubscriberIaVolume(subscriberId = "2", volumeBytes = 3.333D)

    val subscriberIaVolume = sc.parallelize(List(subscriberIaVolume1, subscriberIaVolume2, subscriberIaVolume3))
  }

  "SubscriberIaVolumeDsl" should "get correctly parsed data" in new WithSubscriberIaVolumeText {
    subscriberVolume.toSubscriberIaVolume.count should be (2)
  }

  it should "get errors when parsing data" in new WithSubscriberIaVolumeText {
    subscriberVolume.toSubscriberIaVolumeErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithSubscriberIaVolumeText {
    subscriberVolume.toParsedSubscriberIaVolume.count should be (3)
  }

  it should "get correctly parsed rows" in new WithSubscriberIaVolumeRows {
    rows.toSubscriberIaVolume.count should be (2)
  }

  it should "save in parquet" in new WithSubscriberIaVolume {
    val path = File.makeTemp().name
    subscriberIaVolume.saveAsParquetFile(path)
    sqc.parquetFile(path).toSubscriberIaVolume.collect should be (subscriberIaVolume.collect)
    File(path).deleteRecursively
  }
}
