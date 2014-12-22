/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import scala.reflect.io.Directory

import com.github.nscala_time.time.Imports._
import org.apache.spark.mllib.linalg.Vectors
import org.scalatest._
import sa.com.mobily.parsing.CsvParser
import sa.com.mobily.poi.spark.UserPhoneCallsDsl._
import sa.com.mobily.utils.{LocalSparkContext, EdmCoreUtils}

class UserPhoneCallsTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import UserPhoneCalls._

  trait WithPhoneCallText {

    val phoneCallText = "0500001413|20140824|2541|1|1,2"
    val fields = Array("0500001413","20140824","2541","1","1,2")
    val phoneCallsObjetct =
      UserPhoneCalls(
        500001413,
        new DateTime(2014, 8, 24, 0, 0, EdmCoreUtils.TimeZoneSaudiArabia),
        "2541",
        1,
        Seq(1, 2))
  }

  trait WithWeekPhoneCalls {

    val phoneCall1 =
      UserPhoneCalls(
        1L,
        new DateTime(2014, 8, 24, 0, 0, EdmCoreUtils.TimeZoneSaudiArabia),
        "2541",
        1,
        Seq(0, 1, 2))
    val phoneCall2 = phoneCall1.copy(
      timestamp =
        new DateTime(2014, 8, 18, 0, 0, EdmCoreUtils.TimeZoneSaudiArabia),
      callHours = Seq(0, 23))
    val phoneCall3 = phoneCall1.copy(
      timestamp =
        new DateTime(2014, 8, 19, 0, 0, EdmCoreUtils.TimeZoneSaudiArabia),
      callHours = Seq(1, 23))
    val phoneCall4 =
      UserPhoneCalls(
        2L,
        new DateTime(2014, 8, 25, 0, 0, EdmCoreUtils.TimeZoneSaudiArabia),
        "2566",
        1,
        Seq(1, 2, 3))

    val phoneCalls = sc.parallelize(List(phoneCall1, phoneCall2, phoneCall3, phoneCall4))
    val phoneCallsVector = phoneCalls.perUserAndSiteId.map(element => element._2)
    val clusterNumberAndCostResult = Vector((1,5.000000000000002),(2,0),(3,0))

    def withTemporaryDirectory(testFunction: Directory => Any) {
      val directory = Directory.makeTemp()
      testFunction(directory)
      directory.deleteRecursively()
    }
  }

  trait WithAverageActivity {

    val user1ActivityVectorWeek1 = Vectors.sparse(UserPhoneCalls.HoursInWeek, Seq((0, 1.0), (2, 1.0), (5, 1.0)))
    val user1ActivityVectorWeek2 = Vectors.sparse(UserPhoneCalls.HoursInWeek, Seq((0, 1.0), (5, 1.0)))
    val user1ActivityVectorWeek3 = Vectors.sparse(UserPhoneCalls.HoursInWeek, Seq((0, 1.0)))
    val user1ActivityVectors = Seq(
      user1ActivityVectorWeek1,
      user1ActivityVectorWeek2,
      user1ActivityVectorWeek3)
    val user1ExpectedAverageVector = Vectors.sparse(
      UserPhoneCalls.HoursInWeek,
      Seq(
        (0, 1.0),
        (2, 0.3333333333333333),
        (5, 0.6666666666666666)))
  }

  "UserPhoneCalls" should "be built from CSV" in new WithPhoneCallText {
    CsvParser.fromLine(phoneCallText).value.get should be (phoneCallsObjetct)
  }

  it should "be discarded when the CSV format is wrong" in new WithPhoneCallText {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(3, "WrongRegionId"))
  }

  it should "generate the cluster number and cost sequence" in new WithWeekPhoneCalls {
    val clusterNumberAndCost = generateClusterNumberAndCostSequence(phoneCallsVector, 3)
    clusterNumberAndCost should be(clusterNumberAndCostResult)
  }

  it should "generate the kmeans model graphs" in new WithWeekPhoneCalls {
    withTemporaryDirectory { directory =>
      val numberOfClusters = 3
      val model = kMeansModel(numberOfClusters, phoneCallsVector)
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

  it should "compute the average for activity vectors" in new WithAverageActivity {
    activityAverageVector(user1ActivityVectors) should be(user1ExpectedAverageVector)
  }
}
