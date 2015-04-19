/*
 * TODO: License goes here!
 */

package sa.com.mobily.visit

import com.github.nscala_time.time.Imports._

import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils

case class Footfall(
    users: Set[User],
    location: String,
    interval: Interval,
    frequency: Int,
    recencyCounter: Int,
    avgRecencyInMinutes: Double,
    avgVisitDurationInMinutes: Double,
    avgGeomPrecision: Double) {

  lazy val numDistinctUsers: Int = users.size

  def fields: Array[String] =
    Array(location) ++
      EdmCoreUtils.intervalFields(interval) ++
      Array(
        numDistinctUsers.toString,
        frequency.toString,
        avgRecencyInMinutes.round.toString,
        avgVisitDurationInMinutes.round.toString,
        avgGeomPrecision.round.toString)
}

object Footfall {

  val Header: Array[String] =
    Array("location") ++
      EdmCoreUtils.IntervalHeader ++
      Array("footfall", "frequency", "avgRecencyInMinutes", "avgVisitDurationInMinutes", "avgGeomPrecision")

  def apply(userVisitMetrics: UserVisitMetrics): Footfall =
    Footfall(
      users = Set(userVisitMetrics.user),
      location = userVisitMetrics.location,
      interval = userVisitMetrics.interval,
      frequency = userVisitMetrics.frequency,
      recencyCounter = userVisitMetrics.frequency - 1,
      avgRecencyInMinutes = userVisitMetrics.avgRecencyInMinutes,
      avgVisitDurationInMinutes = userVisitMetrics.avgVisitDurationInMinutes,
      avgGeomPrecision = userVisitMetrics.avgGeomPrecision)

  def aggregate(ff1: Footfall, ff2: Footfall): Footfall = {
    require(ff1.location == ff2.location && ff1.interval == ff2.interval)
    Footfall(
      users = ff1.users ++ ff2.users,
      location = ff1.location,
      interval = ff1.interval,
      frequency = ff1.frequency + ff2.frequency,
      recencyCounter = ff1.recencyCounter + ff2.recencyCounter,
      avgRecencyInMinutes =
        if (ff1.recencyCounter == 0 && ff2.recencyCounter == 0) 0
        else
          (ff1.avgRecencyInMinutes * ff1.recencyCounter + ff2.avgRecencyInMinutes * ff2.recencyCounter) /
            (ff1.recencyCounter + ff2.recencyCounter),
      avgVisitDurationInMinutes =
        (ff1.avgVisitDurationInMinutes * ff1.frequency + ff2.avgVisitDurationInMinutes * ff2.frequency) /
          (ff1.frequency + ff2.frequency),
      avgGeomPrecision =
        (ff1.avgGeomPrecision * ff1.frequency + ff2.avgGeomPrecision * ff2.frequency) / (ff1.frequency + ff2.frequency))
  }
}
