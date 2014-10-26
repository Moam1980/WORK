/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import org.scalatest._
import sa.com.mobily.utils.LocalSparkContext

class SubscriberIaAppsDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import SubscriberIaAppsDsl._

  trait WithSubscriberIaAppsText {

    val subscriberApps1 = "\"20141001\"|\"72541945\"|\"17\"|\"58\"|\"51800\"|\"355520\"|\"407320\"|\"\"|\"101\""
    val subscriberApps2 = "\"20141001\"|\"12345678\"|\"30\"|\"669\"|\"12345\"|\"654321\"|\"987655\"|\"\"|\"203\""
    val subscriberApps3 = "\"20141001\"|\"12345678\"|\"30\"|\"669\"|\"text\"|\"654321\"|\"987655\"|\"\"|\"203\""

    val subscriberApps = sc.parallelize(List(subscriberApps1, subscriberApps2, subscriberApps3))
  }

  "SubscriberIaAppsDsl" should "get correctly parsed data" in new WithSubscriberIaAppsText {
    subscriberApps.toSubscriberIaApps.count should be (2)
  }

  it should "get errors when parsing data" in new WithSubscriberIaAppsText {
    subscriberApps.toSubscriberIaAppsErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithSubscriberIaAppsText {
    subscriberApps.toParsedSubscriberIaApps.count should be (3)
  }
}
