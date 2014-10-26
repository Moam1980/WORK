/*
 * TODO: License goes here!
 */

package sa.com.mobily.dwh.spark

import org.scalatest._
import sa.com.mobily.utils.LocalSparkContext

class SubscriberHajjDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import SubscriberHajjDsl._
  
  trait WithSubscriberHajjText {

    val subscriberHajj1 = "200540000000,N/A,20AUG_10OCT_14,Y,Saudi Arabia,N,Y,0.000,INTERNAL,Consumer,Prepaid," +
      "Connect 5G Pre,Connect,TENURE_GRT90 = \"Y\" and MAKKAH_MADINAH_L3M = \"N\",INTERNAL"
    val subscriberHajj2 = "200540000001,N/A,20AUG_10OCT_14,N,Saudi Arabia,N,Y,0.000,EXTERNAL,Consumer,Prepaid," +
      "Connect 5G Pre,Connect,TENURE_GRT90 = \"Y\" and MAKKAH_MADINAH_L3M = \"N\",EXTERNAL"
    val subscriberHajj3 = "200540000000A,N/A,20AUG_10OCT_14,Y,Saudi Arabia,N,Y,0.000,INTERNAL,Consumer,Prepaid," +
      "Connect 5G Pre,Connect,TENURE_GRT90 = \"YO\" and MAKKAH_MADINAH_L3M = \"N\",INTERNAL"

    val subscriberHajj = sc.parallelize(List(subscriberHajj1, subscriberHajj2,
      subscriberHajj3))
  }

  "SubscriberHajjContext" should "get correctly parsed data" in new WithSubscriberHajjText {
    subscriberHajj.toSubscriberHajj.count should be (2)
  }

  it should "get errors when parsing data" in new WithSubscriberHajjText {
    subscriberHajj.toSubscriberHajjErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithSubscriberHajjText {
    subscriberHajj.toParsedSubscriberHajj.count should be (3)
  }
}
