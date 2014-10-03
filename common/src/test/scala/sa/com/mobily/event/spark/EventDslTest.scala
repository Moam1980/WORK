/*
 * TODO: License goes here!
 */

package sa.com.mobily.event.spark

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.utils.LocalSparkContext

class EventDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import EventDsl._

  trait WithPsEventsText {

    val event1 = "560917079,420034122616618,1,866173010386736,1404162126,1404162610,2,859,100.75.161.156," +
      "173.192.222.168,50101,443,WEB1,84.23.99.177,84.23.99.162,,84.23.99.162,10.210.4.73,1,052C,,330B,,,," +
      "11650,10339,127,110,(null),(null),,,,964,629,"
    val event2 = "560917079,420034122616618,1,866173010386736"
    val event3 = "560917079,420034122616618,1,866173010386736,1404162529,1404162578,16,208,100.75.161.156," +
      "17.149.36.144,50098,5223,WEB1,84.23.99.177,84.23.99.162,,84.23.99.162,10.210.4.73,1,052C,,330B,,,," +
      "5320,5332,26,26,(null),(null),,,,87,833,"

    val events = sc.parallelize(List(event1, event2, event3))
  }

  "EventDsl" should "get correctly parsed psEvents" in new WithPsEventsText {
    events.psToEvent.count should be(2)
  }

  it should "get errors when parsing psEvents" in new WithPsEventsText {
    events.psToEventErrors.count should be(1)
  }

  it should "get both correctly and wrongly parsed psEvents" in new WithPsEventsText {
    events.psToParsedEvent.count should be(3)
  }
}
