/*
 * TODO: License goes here!
 */

package sa.com.mobily.visit

import com.github.nscala_time.time.Imports._

import sa.com.mobily.user.User
import sa.com.mobily.usercentric.Dwell
import sa.com.mobily.utils.EdmCoreUtils

case class UserVisitMetrics(
    user: User,
    location: String,
    interval: Interval,
    frequency: Int,
    durationInBetweenVisits: List[Duration],
    visitDurations: List[Duration],
    geomPrecisions: List[Double]) {

  lazy val avgRecencyInMinutes: Double =
    if (frequency == 1) 0
    else
      durationInBetweenVisits.map(_.getStandardSeconds.toDouble / EdmCoreUtils.SecondsInMinute).sum /
        durationInBetweenVisits.size

  lazy val avgVisitDurationInMinutes: Double =
    visitDurations.map(_.getStandardSeconds.toDouble / EdmCoreUtils.SecondsInMinute).sum / visitDurations.size

  lazy val avgGeomPrecision: Double = geomPrecisions.sum / geomPrecisions.size

  def key: (String, Interval) = (location, interval)

  def fields: Array[String] =
    Array(location) ++
      EdmCoreUtils.intervalFields(interval) ++
      user.fields ++
      Array(
        frequency.toString,
        avgRecencyInMinutes.round.toString,
        avgVisitDurationInMinutes.round.toString,
        avgGeomPrecision.round.toString)
}

object UserVisitMetrics {

  val Header: Array[String] =
    Array("location") ++
      EdmCoreUtils.IntervalHeader ++
      User.Header ++
      Array("frequency", "avgRecencyInMinutes", "avgVisitDurationInMinutes", "avgGeomPrecision")

  def apply(location: String, interval: Interval, userDwellsChronologically: List[Dwell]): UserVisitMetrics = {
    require(userDwellsChronologically.size > 0)
    val durationInBetweenVisits =
      if (userDwellsChronologically.size > 1)
        userDwellsChronologically.sliding(2).map(l => new Duration(l.head.endTime, l.last.startTime)).toList
      else List()
    UserVisitMetrics(
      user = userDwellsChronologically.head.user,
      location = location,
      interval = interval,
      frequency = userDwellsChronologically.size,
      durationInBetweenVisits = durationInBetweenVisits,
      visitDurations = userDwellsChronologically.map(d => new Duration(d.startTime, d.endTime)),
      geomPrecisions = userDwellsChronologically.map(d => d.geom.getArea))
  }
}
