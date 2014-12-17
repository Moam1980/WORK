/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi.spark

import com.github.nscala_time.time.Imports._
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.scalatest._

import sa.com.mobily.cell.{TwoG, EgBts}
import sa.com.mobily.geometry.UtmCoordinates
import sa.com.mobily.poi._
import sa.com.mobily.utils.{EdmCoreUtils, LocalSparkContext}

class UserActivityPoiDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import UserActivityPoiDsl._

  trait WithWeekPhoneCalls {

    val firstUserMsisdn = 1L
    val secondUserMsisdn = 2L
    val firstUserSiteId = "2541"
    val secondUserSiteId = "2566"
    val firstUserRegionId = 10.toShort
    val secondUserRegionId = 20.toShort
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

  trait WithBtsCatalogue extends WithWeekPhoneCalls {

    val coords1 = UtmCoordinates(821375.9, 3086866.0)
    val coords2 = UtmCoordinates(821485.9, 3086976.0)
    val egBts1 = EgBts("6539", "6539", "New-Addition", coords1, "", "", 57, "BTS", "Alcatel", "E317",
      "42003000576539", TwoG, "17", "Macro", 535.49793639, 681.54282813)
    val egBts2 = EgBts("6540", "6540", "New-Addition", coords2, "", "", 57, "BTS", "Alcatel", "E317",
      "42003000576539", TwoG, "17", "Macro", 535.49793639, 681.54282813)
    val btsCatalogue =
      Map(
        (firstUserSiteId, firstUserRegionId) -> Iterable(egBts1, egBts2),
        (secondUserSiteId, secondUserRegionId) -> Iterable(egBts2))
  }

  trait WithUserActivity extends WithWeekPhoneCalls with WithBtsCatalogue {

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
    val pois = userActivity.pois(model, centroidsMapping)
    val userPois = userActivity.userPois(model, centroidsMapping)
  }

  "UserActivityPoiDsl" should "assign the centroids to each entry of the vector" in new WithUserActivity {
    val userActivityPoisList = pois.collect.toList

    userActivityPoisList.length should be(2)
    userActivityPoisList(0) should be((firstUserMsisdn, firstUserSiteId, firstUserRegionId), Work)
    userActivityPoisList(1) should be((secondUserMsisdn, secondUserSiteId, secondUserRegionId), Home)
  }

  it should "group the pois by user" in new WithUserActivity {
    val userPoisList = userPois.collect.toList

    userPoisList.length should be(2)
    userPoisList(0) should be((firstUserMsisdn, List((Work, List((firstUserSiteId, firstUserRegionId))))))
    userPoisList(1) should be((secondUserMsisdn, List((Home, List((secondUserSiteId, secondUserRegionId))))))
  }

  it should "return an RDD with user msisdn, poi type and its geometries" in new WithUserActivity {
    val broadcastCatalogue = sc.broadcast(btsCatalogue)
    val userPoisWithGeomsList = userActivity.userPoisWithGeoms(
      model, centroidsMapping, broadcastCatalogue).collect.toList

    userPoisWithGeomsList.length should be(2)
    userPoisWithGeomsList(0) should be(firstUserMsisdn, Work, Seq(egBts1.geom, egBts2.geom))
    userPoisWithGeomsList(1) should be(secondUserMsisdn, Home, Seq(egBts2.geom))
  }

  it should "return the user Pois and their aggregated geoms applying the default function" in new WithUserActivity {
    val broadcastCatalogue = sc.broadcast(btsCatalogue)
    val userPoisWithGeomsList =
      userActivity.userPoisWithAggregatedGeoms()(model, centroidsMapping, broadcastCatalogue).collect.toList

    userPoisWithGeomsList.length should be(2)
    userPoisWithGeomsList(0) should be(firstUserMsisdn, Work, egBts1.geom.intersection(egBts2.geom))
    userPoisWithGeomsList(1) should be(secondUserMsisdn, Home, egBts2.geom)
  }

  it should "return the user Pois and their aggregated geoms applying another function" in new WithUserActivity {
    val broadcastCatalogue = sc.broadcast(btsCatalogue)
    val userPoisWithGeomsList =
      userActivity.userPoisWithAggregatedGeoms(
        itGeoms => itGeoms.reduce(_.union(_)))(model, centroidsMapping, broadcastCatalogue).collect.toList

    userPoisWithGeomsList.length should be(2)
    userPoisWithGeomsList(0) should be(firstUserMsisdn, Work, egBts1.geom.union(egBts2.geom))
    userPoisWithGeomsList(1) should be(secondUserMsisdn, Home, egBts2.geom)
  }
}
