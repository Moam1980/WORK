/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import org.scalatest._
import sa.com.mobily.utils.LocalSparkContext

class SubscriberIaDomainsDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import SubscriberIaDomainsDsl._

  trait WithSubscriberIaDomainsText {

    val subscriberDomains1 = "\"20141001\"|\"15105283\"|\"log.advertise.1mobile.com\"|\"advertise.1mobile.com\"|" +
      "\"45\"|\"28378\"|\"20492\"|\"48870\"|\"\"|\"101\""
    val subscriberDomains2 = "\"20141001\"|\"12345678\"|\"aaaaaaaaa.aaaaaa.com.sa\"|\"advertise.aaaaa.com.sa\"|" +
      "\"77\"|\"65432\"|\"76543\"|\"98765\"|\"\"|\"OTRO\""
    val subscriberDomains3 = "\"20141001\"|\"15105283\"|\"log.advertise.1mobile.com\"|\"advertise.1mobile.com\"|" +
      "\"45\"|\"28378\"|\"20492\"|\"text\"|\"\"|\"101\""

    val subscriberDomains = sc.parallelize(List(subscriberDomains1, subscriberDomains2, subscriberDomains3))
  }

  "SubscriberIaDomainsDsl" should "get correctly parsed data" in new WithSubscriberIaDomainsText {
    subscriberDomains.toSubscriberIaDomains.count should be (2)
  }

  it should "get errors when parsing data" in new WithSubscriberIaDomainsText {
    subscriberDomains.toSubscriberIaDomainsErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithSubscriberIaDomainsText {
    subscriberDomains.toParsedSubscriberIaDomains.count should be (3)
  }
}
