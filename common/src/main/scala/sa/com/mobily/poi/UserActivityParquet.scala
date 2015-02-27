/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import org.apache.spark.sql._

import sa.com.mobily.parsing.RowParser
import sa.com.mobily.user.User

case class UserActivityParquet(
    user: User,
    siteId: String,
    regionId: Short,
    weekHoursWithActivity: Map[Int, Double],
    weekYear: Seq[(Short, Short)]) {

  def toUserActivity: UserActivity = UserActivity(this)
}

object UserActivityParquet {

  def apply(ua: UserActivity): UserActivityParquet =
    UserActivityParquet(ua.user, ua.siteId, ua.regionId, ua.weekHoursWithActivity, ua.weekYear.toSeq)

  implicit val fromRow = new RowParser[UserActivityParquet] {

    override def fromRow(row: Row): UserActivityParquet = {
      val Seq(userRow, siteId, regionId, weekHoursWithActivityRow, weekYearRow) = row.toSeq
      val user = User.fromRow.fromRow(userRow.asInstanceOf[Row])
      val weekHoursWithActivity = weekHoursWithActivityRow.asInstanceOf[Map[Row, Row]]
        .map(values => (values._1.asInstanceOf[Int], values._2.asInstanceOf[Double]))
      val weekYear = weekYearRow.asInstanceOf[Seq[Row]].map(values => (values.getShort(0), values.getShort(1)))

      UserActivityParquet(
        user = user,
        siteId = siteId.asInstanceOf[String],
        regionId = regionId.asInstanceOf[Short],
        weekHoursWithActivity = weekHoursWithActivity,
        weekYear = weekYear)
    }
  }
}
