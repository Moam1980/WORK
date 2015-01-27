/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import sa.com.mobily.cell.Cell
import sa.com.mobily.geometry.GeomUtils
import sa.com.mobily.utils.EdmCoreUtils

case class CompatibilityScore(
    intersectionRatio: Double,
    hoursDiff: Double,
    minSpeed: Double) extends Ordered[CompatibilityScore] {

  // TODO: Will need to learn it from data
  lazy val timeDifferenceRatio = CompatibilityScore.TimeDiffWeight / (hoursDiff + 1)

  lazy val ratio =
    if ((intersectionRatio == 0) || (minSpeed > Journey.AvgWalkingSpeed)) 0
    else (intersectionRatio * CompatibilityScore.IntersectionWeight) + timeDifferenceRatio

  def compare(another: CompatibilityScore): Int = ratio.compare(another.ratio)
}

object CompatibilityScore {

  val IntersectionWeight = 0.65
  val TimeDiffWeight = 0.35

  def score(first: SpatioTemporalSlot, second: SpatioTemporalSlot)
      (implicit cellCatalogue: Map[(Int, Int), Cell]): CompatibilityScore = {
    val hoursDiff = (second.startTime - first.endTime) / (EdmCoreUtils.MillisInSecond * EdmCoreUtils.SecondsInHour)
    CompatibilityScore(GeomUtils.intersectionRatio(first.geom, second.geom), hoursDiff, first.outMinSpeed)
  }
}
