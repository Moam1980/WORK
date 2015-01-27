/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import scala.annotation.tailrec
import scala.util.Try

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.geom.util.PolygonExtracter
import com.vividsolutions.jts.simplify.DouglasPeuckerSimplifier

import sa.com.mobily.cell.Cell
import sa.com.mobily.event.Event
import sa.com.mobily.geometry.{Coordinates, GeomUtils}

trait CellsGeometry {

  require(!cells.isEmpty)

  val cells: Set[(Int, Int)]

  def geom(implicit cellCatalogue: Map[(Int, Int), Cell]): Geometry = {
    val firstCell = cells.head
    val firstGeom = firstCell.coverageGeom
    val remainingGeoms = (cells - firstCell).map(_.coverageGeom)
    val candidate = remainingGeoms.foldLeft(firstGeom)((accumGeom, cellGeom) =>
      Try { accumGeom.intersection(cellGeom) }.toOption.getOrElse(accumGeom))
    val simplifiedCandidate = firstGeom.getFactory.buildGeometry(PolygonExtracter.getPolygons(
      DouglasPeuckerSimplifier.simplify(candidate, GeomUtils.SimplifyGeomTolerance)))
    if (simplifiedCandidate.isValid && !simplifiedCandidate.isEmpty) simplifiedCandidate
    else {
      val unionCandidate = remainingGeoms.foldLeft(firstGeom)((accumGeom, cellGeom) =>
        Try { accumGeom.union(cellGeom) }.toOption.getOrElse(accumGeom))
      firstGeom.getFactory.buildGeometry(PolygonExtracter.getPolygons(
        DouglasPeuckerSimplifier.simplify(unionCandidate, GeomUtils.SimplifyGeomTolerance)))
    }
  }
}

trait CountryGeometry {

  val geomWkt: String
  val countryIsoCode: String

  lazy val geom: Geometry = GeomUtils.parseWkt(geomWkt, Coordinates.isoCodeUtmSrid(countryIsoCode))
}

object UserModel {

  @tailrec
  def aggTemporalOverlapAndSameCell(
      events: List[Event],
      previous: Option[SpatioTemporalSlot] = None,
      result: List[SpatioTemporalSlot] = List())
      (implicit cellCatalogue: Map[(Int, Int), Cell]): List[SpatioTemporalSlot] = events match {
    case Nil => result ++ previous
    case event :: tail if previous.isDefined =>
      if ((previous.get.endTime > event.beginTime) || previous.get.cells.contains((event.lacTac, event.cellId)))
        aggTemporalOverlapAndSameCell(tail, Some(previous.get.append(event)), result)
      else
        aggTemporalOverlapAndSameCell(tail, Some(SpatioTemporalSlot(event)), result :+ previous.get)
    case event :: tail if !previous.isDefined =>
      aggTemporalOverlapAndSameCell(tail, Some(SpatioTemporalSlot(event)), result)
  }

  @tailrec
  def computeScores(
      slots: List[SpatioTemporalSlot],
      result: List[SpatioTemporalSlot] = List())
      (implicit cellCatalogue: Map[(Int, Int), Cell]): List[SpatioTemporalSlot] = slots match {
    case Nil => result
    case onlySlot :: Nil => result :+ onlySlot
    case first :: second :: tail =>
      computeScores(
        second :: tail,
        result :+ first.copy(score = Some(CompatibilityScore.score(first, second))))
  }

  @tailrec
  def aggregateCompatible(slots: List[SpatioTemporalSlot])
      (implicit cellCatalogue: Map[(Int, Int), Cell]): List[SpatioTemporalSlot] = {
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
        val mergedSlot =
          if (slots.isDefinedAt(2))
            mergedSlotNoScore.copy(score = Some(CompatibilityScore.score(mergedSlotNoScore, slots(2))))
          else mergedSlotNoScore
        aggregateCompatible(List(mergedSlot) ++ slots.drop(2))
      }
    }
  }

  @tailrec
  def fillViaPoints(
      slots: List[SpatioTemporalSlot],
      result: List[SpatioTemporalSlot] = List())
      (implicit cellCatalogue: Map[(Int, Int), Cell]): List[SpatioTemporalSlot] = slots match {
    case Nil => result
    case onlySlot :: Nil => result :+ onlySlot
    case first :: second :: tail =>
      if (first.outMinSpeed > Journey.AvgWalkingSpeed && second.outMinSpeed > Journey.AvgWalkingSpeed)
        fillViaPoints(
          slots = List(second.copy(typeEstimate = JourneyViaPointEstimate)) ++ tail,
          result = result :+ first)
      else
        fillViaPoints(
          slots = List(second) ++ tail,
          result = result :+ first)
  }

  @tailrec
  def userCentric(
      slots: List[SpatioTemporalSlot],
      dwellsResult: List[Dwell] = List(),
      journeysResult: List[Journey] = List(),
      jvpResult: List[JourneyViaPoint] = List(),
      journeyIdSeq: Int = 0)
      (implicit cellCatalogue: Map[(Int, Int), Cell]): (List[Dwell], List[Journey], List[JourneyViaPoint]) =
    slots match {
      case Nil => (dwellsResult, journeysResult, jvpResult)
      case onlySlot :: Nil => (dwellsResult :+ Dwell(onlySlot), journeysResult, jvpResult)
      case first :: tail =>
        val (viaPointsSlots, remaining) = tail.span(_.typeEstimate == JourneyViaPointEstimate)
        val viaPoints = viaPointsSlots.map(s => JourneyViaPoint(s, journeyIdSeq))
        userCentric(
          slots = remaining,
          dwellsResult = dwellsResult :+ Dwell(first),
          journeysResult =
            journeysResult :+ Journey(orig = first, dest = remaining.head, id = journeyIdSeq, viaPoints = viaPoints),
          jvpResult = jvpResult ++ viaPoints,
          journeyIdSeq = journeyIdSeq + 1)
    }
}
