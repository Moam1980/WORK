/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.joda.time.format.DateTimeFormat

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

  val CentroidTypePrefix = "Type"
  val HoursPerWeek = 168
  val DefaultMinActivityRatio = 0.1
  val DefaultSundayZeroHours = "1970-01-04 00:00:00"
  val TableauDateFormatter = "yyyy-MM-dd HH:mm:ss"
  final val TableauFmt = DateTimeFormat.forPattern(TableauDateFormatter)

  def getRowsOfCentroids(clusterCenters: Array[Vector]): Seq[Array[String]] = {
    clusterCenters.zipWithIndex.flatMap { centroidAndIndex =>
      val date = TableauFmt.parseDateTime(DefaultSundayZeroHours)
      (0 until centroidAndIndex._1.size).map(hour =>
        Array(
          CentroidTypePrefix + centroidAndIndex._2,
          TableauFmt.print(date.plusHours(hour)),
          centroidAndIndex._1(hour).toString))
    }
  }
}
