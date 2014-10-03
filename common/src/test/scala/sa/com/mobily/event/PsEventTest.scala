/*
 * TODO: License goes here!
 */

package sa.com.mobily.event

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.event.PsEvent._

class PsEventTest extends FlatSpec with ShouldMatchers {

  trait WithEvent {

    val event = Event(1404162126L, "null", "null", 1404162610L, 859, 866173010386736L, 420034122616618L, None,
      "052C", None, None, 560917079L, "null", 1, "330B", "null")
    val fields: Array[String] = Array("560917079", "420034122616618", "1", "866173010386736", "1404162126",
      "1404162610", "2", "859", "100.75.161.156", "173.192.222.168", "50101", "443", "WEB1", "84.23.99.177",
      "84.23.99.162", "null", "84.23.99.162", "10.210.4.73", "1", "052C", "null", "330B", "null", "null", "null",
      "11650", "10339", "127", "110", "null", "null", "null", "null", "null", "964", "629", "")
  }

  it should "be built from CSV with a PS event" in new WithEvent {
    fromCsv.fromFields(fields) should be(event)
  }

  it should "be discarded when PS event type is wrong" in new WithEvent {
    an[Exception] should be thrownBy fromCsv.fromFields(fields.updated(7, "A"))
  }
}
