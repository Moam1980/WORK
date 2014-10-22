/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import org.scalatest._
import sa.com.mobily.utils.LocalSparkContext

class SubscriberIaSearchesDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import SubscriberIaSearchesDsl._

  trait WithSubscriberIaSearchesText {

    val subscriberSearches1 = "\"20141001\"|\"62182120\"|\"?????? ?????????? ???????????????????\"|" +
      "\"www.google.com.sa\"|\"1412175468\"|\"google\"|\"Web page\"|\"\"|\"101\""
    val subscriberSearches2 = "\"20141001\"|\"12345678\"|\"?????? ?????????? ???????????????????\"|" +
      "\"www.yahoo.com.sa\"|\"12345678901\"|\"yahoo\"|\"Web page\"|\"LOCATION\"|\"OTHER\""
    val subscriberSearches3 = "\"20141001\"|\"12345678\"|\"?????? ?????????? ???????????????????\"|" +
      "\"www.yahoo.com.sa\"|\"text\"|\"yahoo\"|\"Web page\"|\"LOCATION\"|\"OTHER\""

    val subscriberSearches = sc.parallelize(List(subscriberSearches1, subscriberSearches2, subscriberSearches3))
  }

  "SubscriberIaSearchesDsl" should "get correctly parsed data" in new WithSubscriberIaSearchesText {
    subscriberSearches.toSubscriberIaSearches.count should be (2)
  }

  it should "get errors when parsing data" in new WithSubscriberIaSearchesText {
    subscriberSearches.toSubscriberIaSearchesErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithSubscriberIaSearchesText {
    subscriberSearches.toParsedSubscriberIaSearches.count should be (3)
  }
}
