/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import org.scalatest._
import sa.com.mobily.utils.LocalSparkContext

class SubscriberIaAppsCategoriesDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import SubscriberIaAppsCategoriesDsl._

  trait WithSubscriberIaAppsCategoriesText {

    val subscriberAppsCategories1 = "\"20141001\"|\"12556247\"|\"18\"|\"1\"|\"3942\"|\"10976\"|\"14918\"|\"\"|\"101\""
    val subscriberAppsCategories2 = "\"20141001\"|\"12556247\"|\"444\"|\"1111\"|\"123456\"|\"9876543\"|\"14918\"|\"" +
      "\"2121|\"101\""
    val subscriberAppsCategories3 = "\"20141001\"|\"12556247\"|\"18\"|\"1\"|\"3942\"|\"text\"|\"14918\"|\"\"|\"101\""

    val subscriberAppsCategories =
      sc.parallelize(List(subscriberAppsCategories1, subscriberAppsCategories2, subscriberAppsCategories3))
  }

  "SubscriberIaAppsCategoriesDsl" should "get correctly parsed data" in new WithSubscriberIaAppsCategoriesText {
    subscriberAppsCategories.toSubscriberIaAppsCategories.count should be (2)
  }

  it should "get errors when parsing data" in new WithSubscriberIaAppsCategoriesText {
    subscriberAppsCategories.toSubscriberIaAppsCategoriesErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithSubscriberIaAppsCategoriesText {
    subscriberAppsCategories.toParsedSubscriberIaAppsCategories.count should be (3)
  }
}
