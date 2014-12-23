/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import org.apache.spark.mllib.linalg.Vectors
import org.scalatest._

import sa.com.mobily.user.User
import sa.com.mobily.utils.LocalSparkContext

class UserActivityTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  trait WithAverageActivity {

    val user1ActivityVectorWeek1 = Vectors.sparse(UserActivityCdr.HoursInWeek, Seq((0, 1.0), (2, 1.0), (5, 1.0)))
    val user1ActivityVectorWeek2 = Vectors.sparse(UserActivityCdr.HoursInWeek, Seq((0, 1.0), (5, 1.0)))
    val user1ActivityVectorWeek3 = Vectors.sparse(UserActivityCdr.HoursInWeek, Seq((0, 1.0)))
    val user1ActivityVectors = Seq(
      user1ActivityVectorWeek1,
      user1ActivityVectorWeek2,
      user1ActivityVectorWeek3)
    val user1ExpectedAverageVector = Vectors.sparse(
      UserActivityCdr.HoursInWeek,
      Seq(
        (0, 1.0),
        (2, 0.3333333333333333),
        (5, 0.6666666666666666)))
  }

  trait WithUser extends WithAverageActivity {

    val imei = "imeitest"
    val imsi = "imsitest"
    val msisdn = 1L
    val siteId = "siteidtest"
    val regionId = 123.toShort
    val user = User(imei, imsi, msisdn)
    val userActivity = UserActivity(user, siteId, regionId, user1ActivityVectorWeek1)
    val expectedKey = (user, siteId, regionId)
  }

  "UserActivity" should "return the proper key" in new WithUser {
    userActivity.key should be(expectedKey)
  }

  it should "compute the average for activity vectors" in new WithAverageActivity {
    UserActivity.activityAverageVector(user1ActivityVectors) should be(user1ExpectedAverageVector)
  }
}
