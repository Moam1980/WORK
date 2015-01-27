/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import org.apache.spark.mllib.linalg.{Vectors, Vector}

import sa.com.mobily.user.User

case class UserActivity(
    user: User,
    siteId: String,
    regionId: Short,
    weekHoursWithActivity: Map[Int, Double],
    weekYear: Set[(Short, Short)]) {

  lazy val key: (User, String, Short) = (user, siteId, regionId)

  lazy val keyByWeek: (User, String, Short, Set[(Short, Short)]) = (user, siteId, regionId, weekYear)

  lazy val activityVector: Vector =
    Vectors.sparse(
      UserActivity.HoursPerWeek,
      weekHoursWithActivity.map(
        hourWeekActivity => (hourWeekActivity._1, hourWeekActivity._2 / weekYear.size)).toSeq)

  def aggregate(other: UserActivity): UserActivity = {
    other.copy(
      weekYear = weekYear ++ other.weekYear,
      weekHoursWithActivity = weekHoursWithActivity ++ other.weekHoursWithActivity.map(
        dayActivity => (dayActivity._1, dayActivity._2 + weekHoursWithActivity.getOrElse(dayActivity._1, 0D))))
  }

  def combineByWeekYear(other: UserActivity): UserActivity =
    other.copy(weekHoursWithActivity = weekHoursWithActivity ++ other.weekHoursWithActivity)
}

object UserActivity {

  val HoursPerWeek = 168
  val DefaultMinActivityRatio = 0.1
}
