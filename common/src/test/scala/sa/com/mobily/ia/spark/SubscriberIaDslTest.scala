/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import org.scalatest._
import sa.com.mobily.utils.LocalSparkContext

class SubscriberIaDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import SubscriberIaDsl._

  trait WithSubscriberIaText {

    val subscriber1 = "\"18711223\"|\"966560000000\"|\"1\"|\"19810723\"|\"\"|\"966\"|\"\"|\"2\"|\"33\"|\"\"|" +
      "\"\"|\"\"|\"\"|\"\"|\"\"|\"7\""
    val subscriber2 = "\"12345678\"|\"966561111111\"|\"44\"|\"19810723\"|\"\"|\"966\"|\"\"|\"4\"|\"53\"|\"\"|" +
      "\"\"|\"\"|\"\"|\"\"|\"\"|\"7\""
    val subscriber3 = "\"18711223\"|\"WrongNumber\"|\"1\"|\"19810723\"|\"\"|\"966\"|\"\"|\"2\"|\"33\"|\"\"|" +
      "\"\"|\"\"|\"\"|\"\"|\"\"|\"7\""

    val subscriber = sc.parallelize(List(subscriber1, subscriber2,
      subscriber3))
  }

  "SubscriberIaDsl" should "get correctly parsed data" in new WithSubscriberIaText {
    subscriber.toSubscriberIa.count should be (2)
  }

  it should "get errors when parsing data" in new WithSubscriberIaText {
    subscriber.toSubscriberIaErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed data" in new WithSubscriberIaText {
    subscriber.toParsedSubscriberIa.count should be (3)
  }
}
