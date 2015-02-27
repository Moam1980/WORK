package sa.com.mobily.poi.spark

import scala.reflect.io.File

import org.apache.spark.rdd.RDD
import org.scalatest._

import sa.com.mobily.poi.UserActivity
import sa.com.mobily.user.User
import sa.com.mobily.utils.LocalSparkSqlContext

class UserActivityDslTest extends FlatSpec with ShouldMatchers with LocalSparkSqlContext {

  import UserActivityDsl._

  trait WithUserActivity {

    val user1 = User("", "", msisdn = 1L)
    val siteId1 = "siteIdTest"
    val regionId1 = 1.toShort
    val weekHoursWithActivity1 = Map(
      (0, 1D), (1, 1D), (2, 1D), (3, 1D), (4, 1D), (5, 1D), (10, 1D), (11, 1D), (12, 1D), (13, 1D), (14, 1D), (15, 1D),
      (20, 1D), (21, 1D), (22, 1D), (23, 1D), (24, 1D))
    val weekHoursWithActivity2 = Map((166, 1D), (167, 1D))

    val weekYear1 = Set((2014.toShort, 1.toShort))
    val weekYear2 = Set((2014.toShort, 2.toShort))
    val weekYear3 = Set((2014.toShort, 3.toShort))

    val userActivity1 = UserActivity(
      user = user1,
      siteId = siteId1,
      regionId = regionId1,
      weekHoursWithActivity = weekHoursWithActivity1,
      weekYear = weekYear1)
    val userActivity2 = userActivity1.copy(weekHoursWithActivity = weekHoursWithActivity2)
    val userActivity3 = userActivity1.copy(weekYear = weekYear2)
    val userActivity4 = userActivity2.copy(weekYear = weekYear2)
    val userActivity5 = userActivity2.copy(weekYear = weekYear3)

    val userActivities : RDD[UserActivity] = sc.parallelize(
      List(userActivity1, userActivity2, userActivity3, userActivity4, userActivity5))
  }

  "UserActivityDsl" should "compute the aggregate for activity vectors" in new WithUserActivity {
    userActivities.byYearWeek.aggregateActivity.collect.length should be (1)
  }

  it should "compute the aggregate for weekly activity and overriding default ratio" in new WithUserActivity {
    userActivities.byYearWeek.aggregateActivity.removeLittleActivity(0.2).collect.length should be (0)
  }

  it should "filter activities user with low activity" in new WithUserActivity {
    userActivities.byYearWeek.aggregateActivity.removeLittleActivity(0.1).collect.length should be (1)
  }

  it should "group activities by weekYear" in new WithUserActivity {
    userActivities.byYearWeek.collect.length should be (3)
  }

  it should "save UserActivity in parquet" in new WithUserActivity {
    val path = File.makeTemp().name
    userActivities.saveAsParquetFile(path)
    sqc.parquetFile(path).toUserActivity.collect.sameElements(userActivities.collect) should be (true)
    File(path).deleteRecursively
  }
}
