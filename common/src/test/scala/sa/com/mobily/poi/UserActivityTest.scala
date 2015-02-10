/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.scalatest._

import sa.com.mobily.user.User
import sa.com.mobily.utils.LocalSparkContext

class UserActivityTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  trait WithUsersBtsRegionId {

    val user1 = User("imeitest", "imsitest", 1L)
    val user2 = user1.copy(msisdn = 2L)
    val siteId = "siteidtest"
    val regionId = 123.toShort
  }

  trait WithAverageActivity extends WithUsersBtsRegionId {
    val user1ActivityVectorWeek = Map((0, 3.0), (2, 1.0), (5, 2.0))
    val set = Set((2014.toShort, 1.toShort), (2014.toShort, 2.toShort), (2014.toShort, 3.toShort))
    val expectedKey = (user1, siteId, regionId)
    val userActivity1 = UserActivity(user1, siteId, regionId, user1ActivityVectorWeek, set)
    val user1ExpectedAverageVector = Vectors.sparse(
      UserActivityCdr.HoursInWeek,
      Seq(
        (0, 1.0),
        (2, 0.3333333333333333),
        (5, 0.6666666666666666)))
  }
  
  trait WithSameWeek extends WithUsersBtsRegionId {

    val user1ActivityVectorWeek1 = Map((0, 0.5), (2, 0.5), (4, 0.5))
    val user1ActivityVectorWeek2 = Map((4, 0.5), (25, 0.5), (27, 0.5))
    val user1ActivityVectorWeek3 = Map((5, 0.5), (25, 0.5), (27, 0.5))
    val usersCombinerNonOverlappedActivityVector = Map((0, 0.5), (2, 0.5), (4, 0.5), (5, 0.5), (25, 0.5), (27, 0.5))
    val usersCombinerOverlappedActivityVector = Map((0, 0.5), (2, 0.5), (4, 0.5), (25, 0.5), (27, 0.5))
    val userActivityWeek1 = UserActivity(user1, siteId, regionId, user1ActivityVectorWeek1,
      Set((2014.toShort, 1.toShort)))
    val userActivityWeek2 = userActivityWeek1.copy(weekHoursWithActivity = user1ActivityVectorWeek2)
    val userActivityWeek3 = userActivityWeek1.copy(weekHoursWithActivity = user1ActivityVectorWeek3)
    val combinerUserActivity1 = userActivityWeek1.copy(weekHoursWithActivity = usersCombinerOverlappedActivityVector)
    val combinerUserActivity2 = userActivityWeek1.copy(weekHoursWithActivity = usersCombinerNonOverlappedActivityVector)

  }

  trait WithDiferentWeek extends WithUsersBtsRegionId {

    val user1ActivityVectorWeek1 = Map((0, 1.0), (2, 1.0), (4, 1.0))
    val user1ActivityVectorWeek2 = Map((4, 1.0), (25, 1.0), (27, 1.0))
    val user1ActivityVectorWeek3 = Map((5, 1.0), (25, 1.0), (27, 1.0))
    val usersAggregatedNonOverlappedActivityVector = Map((0, 1.0), (2, 1.0), (4, 1.0), (5, 1.0), (25, 1.0), (27, 1.0))
    val usersAggregatedOverlappedActivityVector = Map((0, 1.0), (2, 1.0), (4, 2.0), (25, 1.0), (27, 1.0))
    val weekYear1 = Set((2014.toShort, 1.toShort))
    val weekYear2 = Set((2014.toShort, 2.toShort))
    val combineWeekYear = Set((2014.toShort, 2.toShort), (2014.toShort, 1.toShort))
    val userActivityWeek1 = UserActivity(user1, siteId, regionId, user1ActivityVectorWeek1, weekYear1)
    val userActivityWeek2 = userActivityWeek1.copy(
      weekHoursWithActivity = user1ActivityVectorWeek2, weekYear = weekYear2)
    val userActivityWeek3 = userActivityWeek1.copy(
      weekHoursWithActivity = user1ActivityVectorWeek3, weekYear = weekYear2)
    val aggregatedUserActivity1 = userActivityWeek1.copy(
      weekHoursWithActivity = usersAggregatedOverlappedActivityVector, weekYear = combineWeekYear)
    val aggregatedUserActivity2 = userActivityWeek1.copy(
      weekHoursWithActivity = usersAggregatedNonOverlappedActivityVector, weekYear = combineWeekYear)
    val nonOverlappedActivityVectorAgg = Map((0, 0.5), (2, 0.5), (4, 0.5), (5, 0.5), (25, 0.5), (27, 0.5))
    val overlappedActivityVectorAgg = Map((0, 0.5), (2, 0.5), (4, 1.0), (25, 0.5), (27, 0.5))
    val activityVectorNonOverlapped = Vectors.sparse(UserActivity.HoursPerWeek, nonOverlappedActivityVectorAgg.toSeq)
    val activityVectorOverlapped = Vectors.sparse(UserActivity.HoursPerWeek, overlappedActivityVectorAgg.toSeq)
  }

  trait WithAverageCalculation extends WithUsersBtsRegionId {
    val user1ActivityVectorWeek1 = Map((0, 1.0), (2, 1.0), (4, 1.0))
    val user1ActivityVectorWeek2 = Map((3, 1.0), (4, 1.0), (5, 1.0))
    val user1ActivityVectorWeek3 = Map((3, 1.0), (5, 1.0), (6, 1.0))
    val weekYear1 = Set((2014.toShort, 1.toShort))
    val weekYear2 = Set((2014.toShort, 2.toShort))
    val weekYear3 = Set((2014.toShort, 3.toShort))
    val userActivityWeek1 = UserActivity(user1, siteId, regionId, user1ActivityVectorWeek1, weekYear1)
    val userActivityWeek2 = UserActivity(user1, siteId, regionId, user1ActivityVectorWeek2, weekYear2)
    val userActivityWeek3 = UserActivity(user1, siteId, regionId, user1ActivityVectorWeek3, weekYear3)
  }

  trait WithClusterCentersVectors {
    val vector0 = Vectors.zeros(0)
    val emptyKmeansArrayVectors = Array(vector0)
    val vector1 = Vectors.zeros(3)
    val vector2 = Vectors.zeros(3)
    val kmeansArrayVectors = Array(vector1, vector2)
    val resultSeq: Seq[String] = Seq(
      Array("Type0", "1970-01-04 00:00:00", "0.0"), Array("Type0", "1970-01-04 01:00:00", "0.0"),
      Array("Type0", "1970-01-04 02:00:00", "0.0"), Array("Type1", "1970-01-04 00:00:00", "0.0"),
      Array("Type1", "1970-01-04 01:00:00", "0.0"), Array("Type1", "1970-01-04 02:00:00", "0.0")).map(_.mkString(","))
  }

  "UserActivity" should "return the proper key" in new WithAverageActivity {
    userActivity1.key should be(expectedKey)
  }

  it should "compute the average for activity vectors" in new WithAverageActivity {
    userActivity1.activityVector should be(user1ExpectedAverageVector)
  }

  it should "combine two user activities with overlapping hours in the same week" in new WithSameWeek {
    val combinedActivities = userActivityWeek1.combineByWeekYear(userActivityWeek2)
    (combinedActivities.weekHoursWithActivity, combinedActivities.weekYear) should
      be (combinerUserActivity1.weekHoursWithActivity, combinerUserActivity1.weekYear)
  }

  it should "combine two user activities with non overlapping hours in the same week" in new WithSameWeek {
    val combinedActivities = userActivityWeek1.combineByWeekYear(userActivityWeek3)
    (combinedActivities.weekHoursWithActivity, combinedActivities.weekYear) should
      be (combinerUserActivity2.weekHoursWithActivity, combinerUserActivity2.weekYear)
  }

  it should "aggregate two user activities with overlapping hours in diferent weeks" in new WithDiferentWeek {
    val aggregatedActivities = userActivityWeek1.aggregate(userActivityWeek2)
    (aggregatedActivities.weekHoursWithActivity, aggregatedActivities.weekYear) should
      be (aggregatedUserActivity1.weekHoursWithActivity, aggregatedUserActivity1.weekYear)
  }

  it should "aggregate two user activities with non overlapping hours in diferent weeks" in new WithDiferentWeek {
    val combinedActivities = userActivityWeek1.aggregate(userActivityWeek3)
    (combinedActivities.weekHoursWithActivity, combinedActivities.weekYear) should
      be (aggregatedUserActivity2.weekHoursWithActivity, aggregatedUserActivity2.weekYear)
  }

  it should "calculate the user activities average with non overlapped hours" in new WithDiferentWeek {
    val aggregatedActivities = userActivityWeek1.aggregate(userActivityWeek3)
    aggregatedActivities.activityVector should be (activityVectorNonOverlapped)
  }

  it should "calculate the user activities average with overlapped hours" in new WithDiferentWeek {
    val aggregatedActivities = userActivityWeek1.aggregate(userActivityWeek2)
    aggregatedActivities.activityVector should be (activityVectorOverlapped)
  }

  it should "calculate the same average A.avg(B).avg(C) than C.avg(B).avg(A)" in new WithAverageCalculation {
    val activityVector1 = 
      userActivityWeek1.aggregate(userActivityWeek2).aggregate(userActivityWeek3).activityVector
    val activityVector2 =
      userActivityWeek3.aggregate(userActivityWeek2).aggregate(userActivityWeek1).activityVector
    activityVector1 should be (activityVector2)
  }

  it should "create an array of formatted strings of cluster centers vectors" in new WithClusterCentersVectors {
    UserActivity.getRowsOfCentroids(kmeansArrayVectors).map(_.mkString(",")) should be (resultSeq)
  }

  it should "create an empty array of formatted strings of cluster centers vectors" in new WithClusterCentersVectors {
    UserActivity.getRowsOfCentroids(emptyKmeansArrayVectors).map(_.mkString(",")) should be (Seq())
  }
}
