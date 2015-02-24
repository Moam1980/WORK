/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.mllib.linalg.{Vectors, Vector}

import sa.com.mobily.cell.EgBts

import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils

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

  def getRowsOfCentroids(clusterCenters: Array[Vector]): Seq[Array[String]] = {
    clusterCenters.zipWithIndex.flatMap { centroidAndIndex =>
      val date = EdmCoreUtils.ViewFmt.parseDateTime(DefaultSundayZeroHours)
      (0 until centroidAndIndex._1.size).map(hour =>
        Array(
          CentroidTypePrefix + centroidAndIndex._2,
          EdmCoreUtils.ViewFmt.print(date.plusHours(hour)),
          centroidAndIndex._1(hour).toString))
    }
  }

  def findGeometries(
      btsIds: Iterable[(String, String)],
      btsCatalogue: Map[(String, String), Iterable[EgBts]]): Iterable[Geometry] = {
    btsIds.flatMap(btsIds => btsCatalogue.get((btsIds._1, btsIds._2)) match {
      case Some(location) => location.map(_.geom)
      case _ => Seq()
    })
  }
}
