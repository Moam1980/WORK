/*
 * TODO: License goes here!
 */

package sa.com.mobily.mobility

import scala.annotation.tailrec

import com.github.nscala_time.time.Imports._

import sa.com.mobily.geometry.GeomUtils
import sa.com.mobily.location.Location
import sa.com.mobily.user.User
import sa.com.mobily.usercentric.Dwell
import sa.com.mobily.utils.EdmCoreUtils

case class MobilityMatrixItem(
    startInterval: Interval,
    endInterval: Interval,
    startLocation: String,
    endLocation: String,
    journeyDuration: Duration,
    numWeeks: Long,
    user: User,
    weight: Double) {

  def fields: Array[String] =
    Array(
      EdmCoreUtils.ViewFmt.print(startInterval.getStart),
      EdmCoreUtils.ViewFmt.print(endInterval.getStart),
      startLocation,
      endLocation,
      journeyDuration.seconds.toString,
      numWeeks.toString) ++
      user.fields :+
      weight.toString
}

object MobilityMatrixItem {

  val WeekDayDateFormatter = "E HH:mm:ss"
  val WeekDayFmt = DateTimeFormat.forPattern(WeekDayDateFormatter)

  val Header =
    Array(
      "StartIntervalInitTime",
      "EndIntervalInitTime",
      "StartLocation",
      "EndLocation",
      "JourneyDurationInSeconds",
      "NumWeeks") ++
      User.Header :+
      "Weight"

  // scalastyle:off method.length
  @tailrec
  def perIntervalAndLocation(
      dwells: List[Dwell],
      timeIntervals: List[Interval],
      locations: List[Location],
      minMinutesInDwell: Int,
      numWeeks: Long,
      results: List[MobilityMatrixItem] = List()): List[MobilityMatrixItem] = dwells match {
    case Nil => results
    case singleElem :: Nil => results
    case first :: second :: tail if first.durationInMinutes < minMinutesInDwell =>
      perIntervalAndLocation(
        dwells = second :: tail,
        timeIntervals = timeIntervals,
        locations = locations,
        minMinutesInDwell = minMinutesInDwell,
        numWeeks = numWeeks,
        results = results)
    case first :: second :: tail if second.durationInMinutes < minMinutesInDwell =>
      perIntervalAndLocation(
        dwells = first :: tail,
        timeIntervals = timeIntervals,
        locations = locations,
        minMinutesInDwell = minMinutesInDwell,
        numWeeks = numWeeks,
        results = results)
    case first :: second :: tail =>
      val matrixItems =
        for (
          startInterval <- timeIntervals.filter(i => i.contains(first.endTime));
          endInterval <- timeIntervals.filter(i => i.contains(second.startTime));
          startLocation <- locations.filter(l => first.geom.intersects(l.geom));
          endLocation <- locations.filter(l => second.geom.intersects(l.geom)))
        yield {
          val origWeight = startLocation.geom.buffer(GeomUtils.DefaultGeomBufferForIntersections).intersection(
            first.geom.buffer(GeomUtils.DefaultGeomBufferForIntersections)).getArea / first.geom.getArea
          val destWeight = endLocation.geom.buffer(GeomUtils.DefaultGeomBufferForIntersections).intersection(
            second.geom.buffer(GeomUtils.DefaultGeomBufferForIntersections)).getArea / second.geom.getArea
          MobilityMatrixItem(
            startInterval = startInterval,
            endInterval = endInterval,
            startLocation = startLocation.name,
            endLocation = endLocation.name,
            journeyDuration = new Duration(first.endTime, second.startTime),
            numWeeks = numWeeks,
            user = first.user,
            weight = (origWeight + destWeight) / 2)
        }
      perIntervalAndLocation(
        dwells = second :: tail,
        timeIntervals = timeIntervals,
        locations = locations,
        minMinutesInDwell = minMinutesInDwell,
        numWeeks = numWeeks,
        results = results ++ matrixItems)
  }
}
