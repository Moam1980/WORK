/*
 * TODO: License goes here!
 */

package sa.com.mobily.event

import org.scalatest.{FlatSpec, ShouldMatchers}

class EventTest extends FlatSpec with ShouldMatchers {

  "Event" should "prefer LAC to TAC" in {
    Event.lacOrTac("1", "2") should be ("1")
  }

  it should "return TAC when LAC is missing" in {
    Event.lacOrTac("", "2") should be ("2")
  }

  it should "prefer SAC to CI" in {
    Event.sacOrCi("1", "2") should be ("1")
  }

  it should "return CI when SAC is missing" in {
    Event.sacOrCi("", "2") should be ("2")
  }
}
