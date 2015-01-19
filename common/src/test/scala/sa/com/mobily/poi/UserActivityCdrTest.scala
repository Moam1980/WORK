/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import scala.reflect.io.Directory

import com.github.nscala_time.time.Imports._
import org.scalatest._

import sa.com.mobily.parsing.CsvParser
import sa.com.mobily.poi.spark.UserActivityCdrDsl._
import sa.com.mobily.user.User
import sa.com.mobily.utils.{LocalSparkContext, EdmCoreUtils}

class UserActivityCdrTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import UserActivityCdr._

  trait WithUserActivityCdrText {

    val userActivityText = "0500001413|20140824|2541|1|1,2"
    val fields = Array("0500001413","20140824","2541","1","1,2")
    val userActivitiesObject =
      UserActivityCdr(
        User("", "", 500001413),
        new DateTime(2014, 8, 24, 0, 0, EdmCoreUtils.TimeZoneSaudiArabia),
        "2541",
        1,
        Seq(1, 2))
  }

  trait WithWeekUserActivities {

    val userActivity1 =
      UserActivityCdr(
        User("", "", 1L),
        new DateTime(2014, 8, 18, 0, 0, EdmCoreUtils.TimeZoneSaudiArabia),
        "2541",
        1,
        Seq(0, 1, 2))
    val userActivitity2 = userActivity1.copy(
      timestamp =
        new DateTime(2014, 8, 19, 0, 0, EdmCoreUtils.TimeZoneSaudiArabia),
      activityHours = Seq(0, 23))
    val userActivity3 =
      UserActivityCdr(
        User("", "", 2L),
        new DateTime(2014, 8, 25, 0, 0, EdmCoreUtils.TimeZoneSaudiArabia),
        "2566",
        1,
        Seq(1, 2, 3))

    val userActivitiesCdr = sc.parallelize(List(userActivity1, userActivitity2, userActivity3))
    val userActivitiesCdrVector = userActivitiesCdr.perUserAndSiteId.map(element => element.activityVector)
    val expectedClusterNumberAndCost = Vector((1,1.9999999999999991),(2,0),(3,0))

    def withTemporaryDirectory(testFunction: Directory => Any) {
      val directory = Directory.makeTemp()
      testFunction(directory)
      directory.deleteRecursively()
    }
  }

  "UserActivityCdr" should "be built from CSV" in new WithUserActivityCdrText {
    CsvParser.fromLine(userActivityText).value.get should be (userActivitiesObject)
  }

  it should "be discarded when the CSV format is wrong" in new WithUserActivityCdrText {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(3, "WrongRegionId"))
  }

  it should "generate the cluster number and cost sequence" in new WithWeekUserActivities {
    val clusterNumberAndCost = generateClusterNumberAndCostSequence(userActivitiesCdrVector, 3)
    clusterNumberAndCost should be(expectedClusterNumberAndCost)
  }

  it should "generate generic graphs" in new WithWeekUserActivities {
    val fileName = "tmp.png"
    withTemporaryDirectory { directory =>
      pngGraph(directory.path.concat(s"/$fileName"), Seq((1,2.0), (2,3.0)))
      directory.list.length should be(1)
    }
  }

  it should "generate the kMeans model graphs" in new WithWeekUserActivities {
    withTemporaryDirectory { directory =>
      val numberOfClusters = 3
      val model = kMeansModel(numberOfClusters, userActivitiesCdrVector)
      kMeansModelGraphs(model, directory.path.concat("/"))
      directory.list.length should be(numberOfClusters)
    }
  }

  it should "translate day-hour pairs into weekly values" in {
    weekHour(1, 0) should be(0)
    weekHour(1, 5) should be(5)
    weekHour(2, 0) should be(24)
    weekHour(7, 23) should be(167)
  }
}
