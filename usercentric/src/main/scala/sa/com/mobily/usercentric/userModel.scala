/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import scala.annotation.tailrec
import scala.util.Try

import com.github.nscala_time.time.Imports._
import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.geom.util.PolygonExtracter
import com.vividsolutions.jts.simplify.DouglasPeuckerSimplifier

import sa.com.mobily.cell.Cell
import sa.com.mobily.event.Event
import sa.com.mobily.geometry.{Coordinates, GeomUtils}
import sa.com.mobily.utils.EdmCoreUtils

trait CellsGeometry {

  require(cells.nonEmpty)

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

  val DistanceThresholdInMeters = 90000
  val UrbanSpeedInMetersPerSecond = 50 / 3.6
  val InterUrbanSpeedInMetersPerSecond = 90 / 3.6
  val DefaultMinDwellDurationInMinutes = 15

  private val LatestHourInDay = 23
  private val LatestMinuteInHour = 59
  private val LatestSecondInMinute = 59
  private val LatestMillisInSecond = 999

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
  def candidatesToViaPoints(
      slots: List[SpatioTemporalSlot],
      isViaPoint: (SpatioTemporalSlot) => Boolean = probableViaPoint,
      result: List[SpatioTemporalSlot] = List()): List[SpatioTemporalSlot] = slots match {
    case Nil => result
    case onlySlot :: Nil => result :+ onlySlot
    case first :: second :: Nil => result :+ first :+ second
    case first :: second :: tail =>
      if (isViaPoint(second))
        candidatesToViaPoints(
          slots = List(second.copy(typeEstimate = JourneyViaPointEstimate)) ++ tail,
          result = result :+ first)
      else
        candidatesToViaPoints(slots = second :: tail, result = result :+ first)
  }

  @tailrec
  def extendTime( // scalastyle:ignore cyclomatic.complexity
      slots: List[SpatioTemporalSlot],
      result: List[SpatioTemporalSlot] = List(),
      firstElem: Boolean = true)
      (implicit cellCatalogue: Map[(Int, Int), Cell]): List[SpatioTemporalSlot] = slots match {
    case Nil => result
    case first :: tail if firstElem =>
      extendTime(
        slots =
          first.copy(startTime = new DateTime(first.startTime).withZone(
            EdmCoreUtils.timeZone(first.countryIsoCode)).withTimeAtStartOfDay.getMillis) :: tail,
        result = result,
        firstElem = false)
    case last :: Nil if !firstElem =>
      result :+
        last.copy(endTime = new DateTime(last.endTime).withZone(EdmCoreUtils.timeZone(last.countryIsoCode)).withTime(
          LatestHourInDay, LatestMinuteInHour, LatestSecondInMinute, LatestMillisInSecond).getMillis)
    case first :: second :: tail =>
      val maxDuration = SpatioTemporalSlot.secondsInBetween(first, second)
      val estimatedDuration = journeyDuration(first.geom.getCentroid.distance(second.geom.getCentroid))
      val journeyTime = math.min(maxDuration, estimatedDuration)
      val halfTimeShift = (maxDuration - journeyTime) / 2
      val (newFirst, newSecond) = (first.typeEstimate, second.typeEstimate) match {
        case (DwellEstimate, DwellEstimate) =>
          (first.copy(endTime = new DateTime(first.endTime).plusSeconds(halfTimeShift).getMillis),
            second.copy(startTime = new DateTime(second.startTime).minusSeconds(halfTimeShift).getMillis))
        case (DwellEstimate, JourneyViaPointEstimate) =>
          (first.copy(endTime = new DateTime(first.endTime).plusSeconds(maxDuration - journeyTime).getMillis), second)
        case (JourneyViaPointEstimate, DwellEstimate) =>
          (first,
            second.copy(startTime = new DateTime(second.startTime).minusSeconds(maxDuration - journeyTime).getMillis))
        case (_, _) => (first, second)
      }
      extendTime(
        slots = newSecond :: tail,
        result = result :+ newFirst,
        firstElem = false)
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

  def journeyDuration(distance: Double): Int = {
    val speed =
      if (distance > DistanceThresholdInMeters) InterUrbanSpeedInMetersPerSecond
      else UrbanSpeedInMetersPerSecond
    (distance / speed).round.toInt
  }

  def probableViaPoint(slotCandidate: SpatioTemporalSlot): Boolean =
    slotCandidate.durationInMinutes < DefaultMinDwellDurationInMinutes
}
