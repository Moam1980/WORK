/*
 * TODO: License goes here!
 */

package sa.com.mobily.roaming.spark

import org.scalatest._

import sa.com.mobily.utils.LocalSparkContext

class InternationalJourneyDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import InternationalJourneyDsl._

  trait WithInternationalJourneysText {

    val internationalJourney1 = "01.10.2014 16:50:13 +966000000000 123456789012345 971000000000 0 5"
    val internationalJourney2 = "10.10.2014 1:00:25 +440000000000 123456789012345 966000000000 1 2"
    val internationalJourney3 = "10.14.2014 14:30:58 +440000000000 text 966000000000 1 2"

    val internationalJourneys =
      sc.parallelize(List(internationalJourney1, internationalJourney2, internationalJourney3))
  }

  "InternationalJourneyDsl" should "get correctly parsed international journey" in new WithInternationalJourneysText {
    internationalJourneys.toInternationalJourney.count should be (2)
  }

  it should "get errors when parsing international journey" in new WithInternationalJourneysText {
    internationalJourneys.toInternationalJourneyErrors.count should be (1)
  }

  it should "get both correctly and wrongly parsed international journey" in new WithInternationalJourneysText {
    internationalJourneys.toParsedInternationalJourney.count should be (3)
  }
}
