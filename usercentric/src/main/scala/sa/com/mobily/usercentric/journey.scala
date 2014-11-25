/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import scala.annotation.tailrec

import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.operation.distance.DistanceOp

import sa.com.mobily.cell.Cell
import sa.com.mobily.event.Event
import sa.com.mobily.geometry.{Coordinates, GeomUtils}
import sa.com.mobily.roaming.CountryCode

case class Journey(
    user: Long,
    id: Int,
    startTime: Long,
    endTime: Long,
    geomWkt: String,
    orderedCells: List[(Int, Int)],
    firstEventBeginTime: Long,
    lastEventEndTime: Long,
    countryIsoCode: String = CountryCode.SaudiArabiaIsoCode) extends CountryGeometry with CellSequence

case class JourneyViaPoint(
    user: Long,
    journeyId: Int,
    startTime: Long,
    endTime: Long,
    geomWkt: String,
    orderedCells: List[(Int, Int)],
    firstEventBeginTime: Long,
    lastEventEndTime: Long,
    countryIsoCode: String = CountryCode.SaudiArabiaIsoCode) extends CountryGeometry with CellSequence

object Journey {

  val MillisInSecond = 1000
  val ZeroSpeed = Some(0.0)

  // scalastyle:off method.length
  def computeMinSpeed(events: List[Event], cells: Map[(Int, Int), Cell]): List[Event] = {
    val geom = Event.geom(cells) _
    val geomFactory = cells.headOption.map(cellTuple =>
      GeomUtils.geomFactory(cellTuple._2.coverageGeom.getSRID, cellTuple._2.coverageGeom.getPrecisionModel)).getOrElse(
        GeomUtils.geomFactory(Coordinates.SaudiArabiaUtmSrid))

    @tailrec
    def fillMinSpeed(
        events: List[Event],
        result: List[Event] = List()): List[Event] = {
      val initPoint =
        GeomUtils.parseWkt(events.head.minSpeedPointWkt.get, geomFactory.getSRID, geomFactory.getPrecisionModel)
      events match {
        case first :: Nil => result :+ first.copy(outSpeed = ZeroSpeed)
        case first :: second :: Nil =>
          val initPointInSecond =
            GeomUtils.ensureNearestPointInGeom(
              geomFactory.createPoint(DistanceOp.nearestPoints(initPoint, geom(second)).last),
              geom(second))
          val speed = DistanceOp.distance(initPoint, initPointInSecond) / secondsInBetween(first, second)
          val newSecond =
            second.copy(
              inSpeed = Some(speed),
              outSpeed = ZeroSpeed,
              minSpeedPointWkt = Some(GeomUtils.wkt(initPointInSecond)))
          result :+ first.copy(outSpeed = Some(speed)) :+ newSecond
        case first :: second :: tail if geom(second).intersects(initPoint) =>
          fillMinSpeed(
            second.copy(inSpeed = ZeroSpeed, minSpeedPointWkt = Some(GeomUtils.wkt(initPoint))) :: tail,
            result :+ first.copy(outSpeed = ZeroSpeed))
        case first :: second :: third :: tail =>
          val closestInSecondToInit =
            GeomUtils.ensureNearestPointInGeom(
              geomFactory.createPoint(DistanceOp.nearestPoints(initPoint, geom(second)).last),
              geom(second))
          val initPointInSecond =
            nextInitPoint(
              closestInSecondToInit = closestInSecondToInit,
              second = geom(second),
              third = geom(third),
              geomFactory = geomFactory)
          val speed = initPoint.distance(initPointInSecond) / secondsInBetween(first, second)
          val newSecond = second.copy(inSpeed = Some(speed), minSpeedPointWkt = Some(GeomUtils.wkt(initPointInSecond)))
          fillMinSpeed(
            newSecond :: third :: tail,
            result :+ first.copy(outSpeed = Some(speed)))
      }
    }

    if (events.isEmpty) List()
    else {
      val startingPoint = geom(events.head).getCentroid // TODO: Might need to refine and get a more appropriate point
      fillMinSpeed(
        events.head.copy(inSpeed = ZeroSpeed, minSpeedPointWkt = Some(GeomUtils.wkt(startingPoint))) :: events.tail)
    }
  }
  // scalastyle:on method.length

  def secondsInBetween(firstEvent: Event, secondEvent: Event): Double = {
    val difference: Double = secondEvent.beginTime - firstEvent.beginTime
    if (difference == 0) 1d / MillisInSecond
    else difference / MillisInSecond
  }

  def nextInitPoint(
      closestInSecondToInit: Point,
      second: Geometry,
      third: Geometry,
      geomFactory: GeometryFactory): Point = {
    val closestInThird = geomFactory.createPoint(DistanceOp.nearestPoints(closestInSecondToInit, third).last)
    val line =
      geomFactory.createLineString(Array(closestInSecondToInit.getCoordinate, closestInThird.getCoordinate))
    val geomThroughSecond = if (line.isValid) line.intersection(second) else closestInSecondToInit
    val closestInLineThroughSecondToThird = DistanceOp.nearestPoints(geomThroughSecond, third).head
    val midPointThroughSecond = geomFactory.createPoint(
      LineSegment.midPoint(closestInSecondToInit.getCoordinate, closestInLineThroughSecondToThird))
    GeomUtils.ensureNearestPointInGeom(
      geomFactory.createPoint(DistanceOp.nearestPoints(second, midPointThroughSecond).head),
      second)
  }
}
