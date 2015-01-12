/*
 * TODO: License goes here!
 */

package sa.com.mobily.dwh.spark

import org.scalatest._

import sa.com.mobily.crm.spark.SubscriberDsl
import sa.com.mobily.utils.LocalSparkContext

class SubscriberDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import SubscriberDsl._
  
  trait WithSubscriberText {

    val customerSubscriber1 = "966544312356|Saudi National ID|1016603803|3581870526733101|M|5049|3|Saudi Arabia|KSA" +
      "|Pre-Paid|Voice|39.75|Retail Customer|A|Active|Siebel|Saudi Arabia|SamsungI930000|100.050000|A" +
      "|A|S50|99.04|68.57|133.77|109.99|106.36|125.23"
    val customerSubscriber2 = "966565366654|Saudi National ID|1022832941|3577590541074623|M|4784|7|Saudi Arabia|KSA" +
      "|Pre-Paid|Voice|30.25|Retail Customer|10/24/2012|Active|MCR|Saudi Arabia|BlackBerryQ1000|74.590000|8/1/2014" +
      "|7/23/2014|S40|55.17|26.81|60.72|64.14|112.18|6.15"
    val customerSubscriber3 = "Invalid Value|IQAMA|2363880648||M|2302|2|Great Britain and N Ireland|" +
      "|Pre-Paid|Data|28.4166666666667|Retail Customer|5/10/2014|Active|MDM|Saudi Arabia|" +
      "SamsungGalaxyTab37.0SM-T21100|0.000000|8/1/2014||W|0|0|0|0|0|0"
    val subscriber = sc.parallelize(List(customerSubscriber1, customerSubscriber2, customerSubscriber3))
  }

  "SubscriberDsl" should "get correctly parsed data" in new WithSubscriberText {
    subscriber.toSubscriber.count should be (2)
  }

  it should "get errors when parsing data" in new WithSubscriberText {
    subscriber.toSubscriberErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithSubscriberText {
    subscriber.toParsedSubscriber.count should be (3)
  }
}
