/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import scala.annotation.tailrec

import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.operation.distance.DistanceOp

import sa.com.mobily.cell.Cell
import sa.com.mobily.geometry.{Coordinates, GeomUtils}
import sa.com.mobily.event.Event

object Journey {

  val MillisInSecond = 1000
  val ZeroSpeed = Some(0.0)

  def computeMinSpeed(events: List[Event], cells: Map[(Int, Int), Cell]): List[Event] = {
    val geom = eventGeom(cells) _
    val geomFactory =
      if (events.headOption.isDefined)
        GeomUtils.geomFactory(geom(events.head).getSRID, geom(events.head).getPrecisionModel)
      else
        GeomUtils.geomFactory(Coordinates.SaudiArabiaUtmSrid)

    @tailrec
    def fillMinSpeed(
        events: List[Event],
        initPoint: Point,
        result: List[Event] = List()): List[Event] = events match {
      case first :: Nil => result :+ zeroOutSpeed(first)
      case first :: second :: Nil => {
        val speed = DistanceOp.distance(initPoint, geom(second)) / secondsInBetween(first, second)
        result :+ first.copy(outSpeed = Some(speed)) :+ second.copy(inSpeed = Some(speed), outSpeed = ZeroSpeed)
      }
      case first :: second :: tail if geom(second).intersects(initPoint) =>
        fillMinSpeed(events = zeroInSpeed(second) :: tail, initPoint = initPoint, result :+ zeroOutSpeed(first))
      case first :: second :: third :: tail => {
        val closestInSecondToInit =
          geomFactory.createPoint(DistanceOp.nearestPoints(initPoint, geom(second)).last)
        val initPointInSecond =
          nextInitPoint(
            closestInSecondToInit = closestInSecondToInit,
            second = geom(second),
            third = geom(third),
            geomFactory = geomFactory)
        val speed = initPoint.distance(initPointInSecond) / secondsInBetween(first, second)
        fillMinSpeed(
          events = second.copy(inSpeed = Some(speed)) :: third :: tail,
          initPoint = initPointInSecond,
          result :+ first.copy(outSpeed = Some(speed)))
      }
    }

    if (events.isEmpty) List()
    else {
      val eventsWithFirstInSpeedZero = zeroInSpeed(events.head) :: events.tail
      val startingPoint = geom(events.head).getCentroid // TODO: Might need to refine and get a more appropriate point
      fillMinSpeed(events = eventsWithFirstInSpeedZero, initPoint = startingPoint)
    }
  }

  def secondsInBetween(firstEvent: Event, secondEvent: Event): Double = {
    val difference: Double = secondEvent.beginTime - firstEvent.beginTime
    if (difference == 0) 1d / MillisInSecond
    else difference / MillisInSecond
  }

  def eventGeom(cells: Map[(Int, Int), Cell])(event: Event): Geometry =
    cells((event.lacTac, event.cellId)).coverageGeom

  def nextInitPoint(
      closestInSecondToInit: Point,
      second: Geometry,
      third: Geometry,
      geomFactory: GeometryFactory): Point = {
    val closestInThird = geomFactory.createPoint(DistanceOp.nearestPoints(closestInSecondToInit, third).last)
    val line =
      geomFactory.createLineString(Array(closestInSecondToInit.getCoordinate, closestInThird.getCoordinate))
    val geomThroughSecond = if (line.isValid) line.intersection(second) else line.getStartPoint.intersection(second)
    val closestInLineThroughSecondToThird = DistanceOp.nearestPoints(geomThroughSecond, third).head
    val midPointThroughSecond = geomFactory.createPoint(
      LineSegment.midPoint(closestInSecondToInit.getCoordinate, closestInLineThroughSecondToThird))
    geomFactory.createPoint(DistanceOp.nearestPoints(second, midPointThroughSecond).head)
  }

  def zeroInSpeed(event: Event): Event = event.copy(inSpeed = ZeroSpeed)

  def zeroOutSpeed(event: Event): Event = event.copy(outSpeed = ZeroSpeed)
}
