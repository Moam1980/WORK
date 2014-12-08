/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import scala.reflect.io.Directory

import com.github.nscala_time.time.Imports._
import org.scalatest._

import sa.com.mobily.parsing.CsvParser
import sa.com.mobily.poi.spark.UserPhoneCallsDsl._
import sa.com.mobily.utils.{LocalSparkContext, EdmCoreUtils}

class UserPhoneCallsTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import UserPhoneCalls._

  trait WhitPhoneCallText {
    val phoneCallText = "0500001413|20140824|2541|1|1,2"
    val fields = Array("0500001413","20140824","2541","1","1,2")
    val phoneCallsObjetct = UserPhoneCalls(500001413, DateTimeFormat.forPattern("yyyymmdd").
      withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140824"), "2541", 1, Seq(1, 2))
  }

  trait WithWeekPhoneCalls{
    val phoneCall1 =
      UserPhoneCalls(
        1L,
        DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140824"),
        "2541",
        1,
        Seq(0, 1, 2))
    val phoneCall2 = phoneCall1.copy(
      timestamp =
        DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140818"),
      callHours = Seq(0, 23))
    val phoneCall3 = phoneCall1.copy(
      timestamp =
        DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140819"),
      callHours = Seq(1, 23))
    val phoneCall4 =
      UserPhoneCalls(
        2L,
        DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140825"),
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

  "UserPhoneCalls" should "be built from CSV" in new WhitPhoneCallText {
    CsvParser.fromLine(phoneCallText).value.get should be (phoneCallsObjetct)
  }

  it should "be discarded when the CSV format is wrong" in new WhitPhoneCallText {
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
