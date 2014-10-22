/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import org.scalatest._
import sa.com.mobily.utils.LocalSparkContext

class SubscriberIaDomainsCategoriesDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import SubscriberIaDomainsCategoriesDsl._

  trait WithSubscriberIaDomainsCategoriesText {

    val subscriberDomainsCategories1 = "\"20141001\"|\"87865881\"|\"100120\"|\"933\"|\"663082\"|\"15365627\"|" +
      "\"16028709\"|\"\"|\"101\""
    val subscriberDomainsCategories2 = "\"20141001\"|\"12345678\"|\"888999\"|\"444\"|\"654321\"|\"87654321\"|" +
      "\"98765432\"|\"EXAMPLE\"|\"OTHER\""
    val subscriberDomainsCategories3 = "\"20141001\"|\"87865881\"|\"100120\"|\"933\"|\"text\"|\"15365627\"|" +
      "\"16028709\"|\"\"|\"101\""

    val subscriberDomainsCategories = sc.parallelize(List(subscriberDomainsCategories1, subscriberDomainsCategories2,
      subscriberDomainsCategories3))
  }

  "SubscriberIaDomainsCategoriesDsl" should "get correctly parsed data" in new WithSubscriberIaDomainsCategoriesText {
    subscriberDomainsCategories.toSubscriberIaDomainsCategories.count should be (2)
  }

  it should "get errors when parsing data" in new WithSubscriberIaDomainsCategoriesText {
    subscriberDomainsCategories.toSubscriberIaDomainsCategoriesErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithSubscriberIaDomainsCategoriesText {
    subscriberDomainsCategories.toParsedSubscriberIaDomainsCategories.count should be (3)
  }
}
