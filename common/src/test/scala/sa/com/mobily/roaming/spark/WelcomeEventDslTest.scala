/*
 * TODO: License goes here!
 */

package sa.com.mobily.roaming.spark

import org.scalatest._
import sa.com.mobily.utils.LocalSparkContext

class WelcomeEventDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import WelcomeDsl._

  trait WithWelcomeEventsText {

    val welcomeEvent1 = "01.10.2014 16:50:13 +966000000000 123456789012345 971000000000 0 5"
    val welcomeEvent2 = "10.10.2014 1:00:25 +440000000000 123456789012345 966000000000 1 2"
    val welcomeEvent3 = "10.14.2014 14:30:58 +440000000000 text 966000000000 1 2"

    val welcomeEvents = sc.parallelize(List(welcomeEvent1, welcomeEvent2, welcomeEvent3))
  }

  "SqmContext" should "get correctly parsed Sqm cells" in new WithWelcomeEventsText {
    welcomeEvents.toWelcomeEvent.count should be (2)
  }

  it should "get errors when parsing Sqm cells" in new WithWelcomeEventsText {
    welcomeEvents.toWelcomeEventErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed Sqm cells" in new WithWelcomeEventsText {
    welcomeEvents.toParsedWelcomeEvent.count should be (3)
  }
}
