/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import scala.annotation.tailrec

import com.vividsolutions.jts.geom.Geometry

import sa.com.mobily.cell.Cell
import sa.com.mobily.event.Event
import sa.com.mobily.geometry.{Coordinates, GeomUtils}

trait CountryGeometry {

  val geomWkt: String
  val countryIsoCode: String

  lazy val geom: Geometry = GeomUtils.parseWkt(geomWkt, Coordinates.isoCodeUtmSrid(countryIsoCode))
}

trait CellSequence {

  val orderedCells: List[(Int, Int)]

  lazy val cells: Set[(Int, Int)] = orderedCells.toSet
}

object UserModel {

  @tailrec
  def aggSameCell(
      events: List[Event],
      previous: Option[SpatioTemporalSlot] = None,
      result: List[SpatioTemporalSlot] = List())
      (implicit cellCatalogue: Map[(Int, Int), Cell]): List[SpatioTemporalSlot] = events match {
    case Nil => result ++ previous
    case event :: tail if previous.isDefined =>
      if (previous.get.events.last.lacTac == event.lacTac && previous.get.events.last.cellId == event.cellId)
        aggSameCell(tail, Some(previous.get.append(event)), result)
      else
        aggSameCell(tail, Some(SpatioTemporalSlot(event)), result :+ previous.get)
    case event :: tail if !previous.isDefined => aggSameCell(tail, Some(SpatioTemporalSlot(event)), result)
  }

  @tailrec
  def computeScores(
      slots: List[SpatioTemporalSlot],
      result: List[SpatioTemporalSlot] = List()): List[SpatioTemporalSlot] = slots match {
    case Nil => result
    case onlySlot :: Nil => result :+ onlySlot
    case first :: second :: tail =>
      computeScores(
        second :: tail,
        result :+ first.copy(score = Some(CompatibilityScore.score(first, second))))
  }

  @tailrec
  def aggregateCompatible(slots: List[SpatioTemporalSlot]): List[SpatioTemporalSlot] = {
    val maxScoreSlot = slots.maxBy(_.score)
    if (!maxScoreSlot.score.isDefined || (maxScoreSlot.score.get.ratio == 0)) slots
    else {
      val maxIndex = slots.indexOf(maxScoreSlot)
      if (slots.isDefinedAt(maxIndex - 1)) {
        val (before, after) = slots.splitAt(maxIndex - 1)
        val mergedItem = after(1).append(after(2))
        val previousToMergedItemWithScore = after(0).copy(score = Some(CompatibilityScore.score(after(0), mergedItem)))
        if (after.isDefinedAt(3)) {
          val mergedItemWithScore = mergedItem.copy(score = Some(CompatibilityScore.score(mergedItem, after(3))))
          aggregateCompatible(before ++ List(previousToMergedItemWithScore, mergedItemWithScore) ++ after.drop(3))
        } else
          aggregateCompatible(before ++ List(previousToMergedItemWithScore, mergedItem))
      } else {
        val mergedSlotNoScore = slots.head.append(slots(1))
        val mergedSlot = mergedSlotNoScore.copy(score = Some(CompatibilityScore.score(mergedSlotNoScore, slots(2))))
        aggregateCompatible(List(mergedSlot) ++ slots.drop(2))
      }
    }
  }
}
