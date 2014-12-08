/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import org.scalatest._
import sa.com.mobily.utils.LocalSparkContext

class SubscriberIaVolumeDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import SubscriberIaVolumeDsl._

  trait WithSubscriberIaVolumeText {

    val subscriberVolume1 = "\"12556247\"|\"18\""
    val subscriberVolume2 = "\"12556247\"|\"444\""
    val subscriberVolume3 = "\"12556247\"|\"NAN\""

    val subscriberVolume = sc.parallelize(List(subscriberVolume1, subscriberVolume2, subscriberVolume3))
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
}
