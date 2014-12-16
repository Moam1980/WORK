/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi.spark

import com.github.nscala_time.time.Imports._
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.scalatest._

import sa.com.mobily.poi._
import sa.com.mobily.utils.{EdmCoreUtils, LocalSparkContext}

class UserActivityPoiDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import UserActivityPoiDsl._

  trait WithWeekPhoneCalls {

    val firstUserMsisdn = 1L
    val secondUserMsisdn = 2L
    val firstUserSiteId = "2541"
    val secondUserSiteId = "2566"
    val firstUserRegionId = 10
    val secondUserRegionId = 20
    val phoneCall1 =
      UserPhoneCalls(
        firstUserMsisdn,
        DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140824"),
        firstUserSiteId,
        firstUserRegionId,
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
        secondUserMsisdn,
        DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140825"),
        secondUserSiteId,
        secondUserRegionId,
        Seq(1, 2, 3))

    val phoneCalls = sc.parallelize(List(phoneCall1, phoneCall2, phoneCall3, phoneCall4))
  }

  trait WithUserActivity extends WithWeekPhoneCalls {

    import UserPhoneCallsDsl._

    val userActivity = phoneCalls.perUserAndSiteId
    val model =
      new KMeansModel(
        Array(
          Vectors.dense(0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
            0.0,0.0,1.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
            0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
            0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
            0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
            0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
            0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0),
          Vectors.dense(1.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
            0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,0.0,
            1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,
            0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
            0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
            0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,
            0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0)))
    val centroidsMapping = Map(0 -> Home, 1 -> Work, 2 -> HighActivity, 3 -> LowActivity)
  }

  "UserPhoneCallsClassifier" should "assign the centroids to each entry of the vector" in new WithUserActivity {
    val userActivityPois = userActivity.pois(model, centroidsMapping).collect.toList

    userActivityPois.length should be(2)
    userActivityPois(0) should be((firstUserMsisdn, firstUserSiteId, firstUserRegionId), Work)
    userActivityPois(1) should be((secondUserMsisdn, secondUserSiteId, secondUserRegionId), Home)
  }
}
