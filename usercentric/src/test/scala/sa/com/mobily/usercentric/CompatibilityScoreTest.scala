/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import org.scalatest.{FlatSpec, ShouldMatchers}

class CompatibilityScoreTest extends FlatSpec with ShouldMatchers {

  trait WithCompatibilityScore {

    val compScoreZeroTimeDiff = CompatibilityScore(0.6, 0)
    val compScoreWithTimeDiff = CompatibilityScore(0.6, 1)
    val compScoreWithNoIntersection = CompatibilityScore(0, 0.2)
  }

  trait WithSpatioTemporalSlots {

    val slot1 = SpatioTemporalSlot(1, 3600000, 7200000, "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", List())
    val slot2 = SpatioTemporalSlot(1, 10800000, 12600000, "POLYGON ((0.5 0, 1.5 0, 1.5 1, 0.5 1, 0.5 0))", List())
    val slot1And2CompatibilityScore = CompatibilityScore(0.5, 1)
  }

  "CompatibilityScore" should "have the maximum time difference ratio when there's no time difference" in
    new WithCompatibilityScore {
      compScoreZeroTimeDiff.timeDifferenceRatio should be(CompatibilityScore.TimeDiffWeight)
    }

  it should "compute time difference ratio" in new WithCompatibilityScore {
    compScoreWithTimeDiff.timeDifferenceRatio should be(CompatibilityScore.TimeDiffWeight / 2)
  }

  it should "compute the CompatibilityScore ratio" in new WithCompatibilityScore {
    compScoreWithTimeDiff.ratio should be(0.565)
  }

  it should "compute the CompatibilityScore ratio when intersection ratio is 0" in new WithCompatibilityScore {
    compScoreWithNoIntersection.ratio should be(0)
  }

  it should "compare with another CompatibilityScore (using only ratio)" in new WithCompatibilityScore {
    compScoreZeroTimeDiff should be > compScoreWithTimeDiff
    compScoreZeroTimeDiff should be >= compScoreZeroTimeDiff
    compScoreWithTimeDiff should be < compScoreZeroTimeDiff
  }

  it should "compute the score of two SpatioTemporalSlot" in new WithSpatioTemporalSlots {
    CompatibilityScore.score(slot1, slot2) should be (slot1And2CompatibilityScore)
  }
}
