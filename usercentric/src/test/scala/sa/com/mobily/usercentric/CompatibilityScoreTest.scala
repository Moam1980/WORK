/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import org.scalatest.{FlatSpec, ShouldMatchers}

import sa.com.mobily.cell.{Cell, FourGFdd, Micro}
import sa.com.mobily.geometry.UtmCoordinates
import sa.com.mobily.user.User

class CompatibilityScoreTest extends FlatSpec with ShouldMatchers {

  trait WithCompatibilityScore {

    val compScoreZeroTimeDiff = CompatibilityScore(0.6, 0, 0)
    val compScoreWithTimeDiff = CompatibilityScore(0.6, 1, 0)
    val compScoreWithNoIntersection = CompatibilityScore(0, 0.2, 0)
    val compScoreWithHighMinSpeed = CompatibilityScore(0.6, 0.2, 5)
  }

  trait WithCellCatalogue {

    val cell1 = Cell(1, 1, UtmCoordinates(1, 4), FourGFdd, Micro, 20, 180, 45, 4, "1",
      "POLYGON ((0 0, 0 20, 20 20, 20 0, 0 0))")
    val cell2 = cell1.copy(cellId = 2, coverageWkt = "POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))")
    val cell3 = cell1.copy(cellId = 3, coverageWkt = "POLYGON ((5 0, 5 10, 15 10, 15 0, 5 0))")

    implicit val cellCatalogue = Map((1, 1) -> cell1, (1, 2) -> cell2, (1, 3) -> cell3)
  }

  trait WithSpatioTemporalSlots {

    val slot1 = SpatioTemporalSlot(User("", "", 1), 3600000, 7200000, Set((1, 1)), 3600000, 7200000, 1.25, 0, 1)
    val slot2 = SpatioTemporalSlot(User("", "", 1), 10800000, 12600000, Set((1, 2)), 10800000, 12600000, 0, 0, 1)
    val slot1And2CompatibilityScore = CompatibilityScore(1, 1, 1.25)
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

  it should "compute the CompatibilityScore ratio when minimum speed is high" in new WithCompatibilityScore {
    compScoreWithHighMinSpeed.ratio should be(0)
  }

  it should "compare with another CompatibilityScore (using only ratio)" in new WithCompatibilityScore {
    compScoreZeroTimeDiff should be > compScoreWithTimeDiff
    compScoreZeroTimeDiff should be >= compScoreZeroTimeDiff
    compScoreWithTimeDiff should be < compScoreZeroTimeDiff
  }

  it should "compute the score of two SpatioTemporalSlot" in new WithSpatioTemporalSlots with WithCellCatalogue {
    CompatibilityScore.score(slot1, slot2) should be (slot1And2CompatibilityScore)
  }
}
