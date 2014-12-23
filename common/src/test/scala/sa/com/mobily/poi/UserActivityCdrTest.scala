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

  trait WithPhoneCallText {

    val phoneCallText = "0500001413|20140824|2541|1|1,2"
    val fields = Array("0500001413","20140824","2541","1","1,2")
    val phoneCallsObjetct =
      UserActivityCdr(
        User("", "", 500001413),
        new DateTime(2014, 8, 24, 0, 0, EdmCoreUtils.TimeZoneSaudiArabia),
        "2541",
        1,
        Seq(1, 2))
  }

  trait WithWeekPhoneCalls {

    val phoneCall1 =
      UserActivityCdr(
        User("", "", 1L),
        new DateTime(2014, 8, 24, 0, 0, EdmCoreUtils.TimeZoneSaudiArabia),
        "2541",
        1,
        Seq(0, 1, 2))
    val phoneCall2 = phoneCall1.copy(
      timestamp =
        new DateTime(2014, 8, 18, 0, 0, EdmCoreUtils.TimeZoneSaudiArabia),
      activityHours = Seq(0, 23))
    val phoneCall3 = phoneCall1.copy(
      timestamp =
        new DateTime(2014, 8, 19, 0, 0, EdmCoreUtils.TimeZoneSaudiArabia),
      activityHours = Seq(1, 23))
    val phoneCall4 =
      UserActivityCdr(
        User("", "", 2L),
        new DateTime(2014, 8, 25, 0, 0, EdmCoreUtils.TimeZoneSaudiArabia),
        "2566",
        1,
        Seq(1, 2, 3))

    val phoneCalls = sc.parallelize(List(phoneCall1, phoneCall2, phoneCall3, phoneCall4))
    val phoneCallsVector = phoneCalls.perUserAndSiteId.map(element => element.activityVector)
    val clusterNumberAndCostResult = Vector((1,5.000000000000002),(2,0),(3,0))

    def withTemporaryDirectory(testFunction: Directory => Any) {
      val directory = Directory.makeTemp()
      testFunction(directory)
      directory.deleteRecursively()
    }
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
}
