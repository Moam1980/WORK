/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import org.apache.spark.sql.Row
import org.scalatest._

import sa.com.mobily.user.User
import sa.com.mobily.utils.LocalSparkContext

class UserActivityParqetTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import UserActivityParquet._

  trait WithUsersBtsRegionId {

    val user1 = User("imeitest", "imsitest", 1L)
    val siteId = "siteidtest"
    val regionId = 123.toShort
  }

  trait WithUserActivityData extends WithUsersBtsRegionId {

    val user1ActivityVectorWeek = Map((0, 3.0), (2, 1.0), (5, 2.0))
    val set = Set((2014.toShort, 1.toShort), (2014.toShort, 2.toShort), (2014.toShort, 3.toShort))
    val userActivity1 = UserActivity(user1, siteId, regionId, user1ActivityVectorWeek, set)
    val userActivity2 = UserActivity(user1, siteId, regionId, Map(), Set())
    val userActivityParquet1 = UserActivityParquet(user1, siteId, regionId, user1ActivityVectorWeek, set.toSeq)
    val userActivityParquet2 = UserActivityParquet(user1, siteId, regionId, Map(), Seq())
    val row1 = Row(Row("imeitest", "imsitest", 1L), "siteidtest", 123.toShort, Map(0 -> 3.0, 2 -> 1.0, 5 -> 2.0),
      Row(Row(2014.toShort, 1.toShort), Row(2014.toShort, 2.toShort), Row(2014.toShort, 3.toShort)))
    val row2 = Row(Row("imeitest", "imsitest", 1L), "siteidtest", 123.toShort, Map(), Row())
    val wrongRow1 = Row(Row("imeitest", "imsitest", 1L), "siteidtest", 123.toShort, Map(55 -> 3.0, 2 -> 1.0, 5 -> 2.0),
      Row(Row(2014.toShort, 1.toShort), Row(2014.toShort, 2.toShort), Row(2014.toShort, 3.toShort)))
    val wrongRow2 = Row(Row("imeitest", "imsitest", 1L), "siteidtest", 123.toShort, Map(0 -> 3.0, 2 -> 1.0, 5 -> 2.0),
      Row(Row(2014.toShort, 1.toShort), Row(2015.toShort, 2.toShort), Row(2014.toShort, 3.toShort)))
  }

  "UserActivityParquet" should "create an UserActivity from UserActivityParquet" in new WithUserActivityData {
    UserActivityParquet(userActivity1) should be (userActivityParquet1)
    UserActivityParquet(userActivity2) should be (userActivityParquet2)
    userActivityParquet1.toUserActivity should be (userActivity1)
    userActivityParquet2.toUserActivity should be (userActivity2)
  }

  it should "be built from row" in new WithUserActivityData {
    fromRow.fromRow(row1) should be (userActivityParquet1)
    fromRow.fromRow(row2) should be (userActivityParquet2)
    fromRow.fromRow(wrongRow1) should not be (userActivityParquet1)
    fromRow.fromRow(wrongRow2) should not be (userActivityParquet1)
  }
}
