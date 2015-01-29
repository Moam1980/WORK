/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi.spark

import com.github.nscala_time.time.Imports._
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.scalatest._

import sa.com.mobily.cell.{EgBts, TwoG}
import sa.com.mobily.geometry.{Coordinates, GeomUtils, UtmCoordinates}
import sa.com.mobily.poi._
import sa.com.mobily.user.User
import sa.com.mobily.utils.{EdmCoreUtils, LocalSparkContext}

class UserActivityPoiDslTest extends FlatSpec with ShouldMatchers with LocalSparkContext {

  import UserActivityPoiDsl._

  trait WithUserActivityCdr {

    val firstUserMsisdn = 1L
    val secondUserMsisdn = 2L
    val firstUserSiteId = "2541"
    val secondUserSiteId = "2566"
    val firstUserRegionId = 1.toShort
    val secondUserRegionId = 2.toShort
    val userActivityCdr1 =
      UserActivityCdr(
        User("", "", firstUserMsisdn),
        DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140824"),
        firstUserSiteId,
        firstUserRegionId,
        Seq(0, 1, 2))
    val userActivityCdr2 = userActivityCdr1.copy(
      timestamp =
        DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140818"),
      activityHours = Seq(0, 23))
    val userActivityCdr3 = userActivityCdr1.copy(
      timestamp =
        DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140819"),
      activityHours = Seq(1, 23))
    val userActivityCdr4 =
      UserActivityCdr(
        User("", "", secondUserMsisdn),
        DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140825"),
        secondUserSiteId,
        secondUserRegionId,
        Seq(1, 2, 3))
    val userActivityCdr5 =
      UserActivityCdr(
        User("", "", secondUserMsisdn),
        DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140825"),
        secondUserSiteId,
        secondUserRegionId,
        activityHours = Seq(1, 23))
    val userActivityCdr6 = userActivityCdr1.copy(
      timestamp =
        DateTimeFormat.forPattern("yyyyMMdd").withZone(EdmCoreUtils.TimeZoneSaudiArabia).parseDateTime("20140819"),
      activityHours = Seq(1, 23),
      siteId = secondUserSiteId,
      regionId = secondUserRegionId)

    val userActivities = sc.parallelize(
      List(userActivityCdr1, userActivityCdr2, userActivityCdr3, userActivityCdr4, userActivityCdr5, userActivityCdr6))
  }

  trait WithBtsCatalogue extends WithUserActivityCdr {

    val coords1 = UtmCoordinates(821375.9, 3086866.0)
    val coords2 = UtmCoordinates(821485.9, 3086976.0)
    val egBts1 = EgBts("6539", "6539", "New-Addition", coords1, "", "", 57, "BTS", "Alcatel", "E317",
      "42003000576539", TwoG, "17", "Macro", 535.49793639, 681.54282813)
    val egBts2 = EgBts("6540", "6540", "New-Addition", coords2, "", "", 57, "BTS", "Alcatel", "E317",
      "42003000576539", TwoG, "17", "Macro", 535.49793639, 681.54282813)
    val btsCatalogue =
      Map(
        (firstUserSiteId, firstUserRegionId.toString) -> Iterable(egBts1, egBts2),
        (secondUserSiteId, secondUserRegionId.toString) -> Iterable(egBts2))
  }

  trait WithUserActivity extends WithUserActivityCdr with WithBtsCatalogue {

    import UserActivityCdrDsl._

    val userActivity = userActivities.toUserActivity
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

    val unionGeom = GeomUtils.parseWkt("POLYGON ((821907.7 3086441.7, 821872.7 3086399.5, 821810.3 3086340.9, " +
      "821741.1 3086290.6, 821666.1 3086249.3, 821586.5 3086217.8, 821503.6 3086196.5, 821418.7 3086185.8, 821333.1 " +
      "3086185.8, 821248.2 3086196.5, 821165.3 3086217.8, 821085.7 3086249.3, 821010.7 3086290.6, 820941.5 3086340.9," +
      " 820879.1 3086399.5, 820824.5 3086465.4, 820778.7 3086537.7, 820742.2 3086615.1, 820715.8 3086696.5, 820699.7" +
      " 3086780.6, 820694.4 3086866, 820699.7 3086951.4, 820715.8 3087035.5, 820742.2 3087116.9, 820778.7 3087194.3," +
      " 820824.5 3087266.6, 820879.1 3087332.5, 820941.5 3087391.1, 820954.1 3087400.3, 820989.1 3087442.5, 821051.5" +
      " 3087501.1, 821120.7 3087551.4, 821195.7 3087592.7, 821275.3 3087624.2, 821358.2 3087645.5, 821443.1 3087656.2," +
      " 821528.7 3087656.2, 821613.6 3087645.5, 821696.5 3087624.2, 821776.1 3087592.7, 821851.1 3087551.4, 821920.3" +
      " 3087501.1, 821982.7 3087442.5, 822037.3 3087376.6, 822083.1 3087304.3, 822119.6 3087226.9, 822146 3087145.5," +
      " 822162.1 3087061.4, 822167.4 3086976, 822162.1 3086890.6, 822146 3086806.5, 822119.6 3086725.1, 822083.1" +
      " 3086647.7, 822037.3 3086575.4, 821982.7 3086509.5, 821920.3 3086450.9, 821907.7 3086441.7))",
      Coordinates.SaudiArabiaUtmSrid)

    val homeGeom = GeomUtils.parseWkt("POLYGON ((822167.4 3086976, 822162.1 3086890.6, 822146 3086806.5, 822119.6 " +
      "3086725.1, 822083.1 3086647.7, 822037.3 3086575.4, 821982.7 3086509.5, 821920.3 3086450.9, 821851.1 3086400.6, " +
      "821776.1 3086359.3, 821696.5 3086327.8, 821613.6 3086306.5, 821528.7 3086295.8, 821443.1 3086295.8, 821358.2 " +
      "3086306.5, 821275.3 3086327.8, 821195.7 3086359.3, 821120.7 3086400.6, 821051.5 3086450.9, 820989.1 3086509.5," +
      " 820934.5 3086575.4, 820888.7 3086647.7, 820852.2 3086725.1, 820825.8 3086806.5, 820809.7 3086890.6, 820804.4" +
      " 3086976, 820809.7 3087061.4, 820825.8 3087145.5, 820852.2 3087226.9, 820888.7 3087304.3, 820934.5 3087376.6," +
      " 820989.1 3087442.5, 821051.5 3087501.1, 821120.7 3087551.4, 821195.7 3087592.7, 821275.3 3087624.2, 821358.2" +
      " 3087645.5, 821443.1 3087656.2, 821528.7 3087656.2, 821613.6 3087645.5, 821696.5 3087624.2, 821776.1 3087592.7," +
      " 821851.1 3087551.4, 821920.3 3087501.1, 821982.7 3087442.5, 822037.3 3087376.6, 822083.1 3087304.3, 822119.6" +
      " 3087226.9, 822146 3087145.5, 822162.1 3087061.4, 822167.4 3086976))", Coordinates.SaudiArabiaUtmSrid)

    val centroidsMapping = Map(0 -> Home, 1 -> Work, 2 -> HighActivity, 3 -> Leisure)
    val pois = userActivity.pois(model, centroidsMapping)
    val userPois = userActivity.userPois(model, centroidsMapping)
  }

  "UserActivityPoiDsl" should "assign the centroids to each entry of the vector" in new WithUserActivity {
    val userActivityPoisList = pois.collect.toList

    userActivityPoisList.length should be(4)
    userActivityPoisList should contain(
      (User("", "", firstUserMsisdn), firstUserSiteId, firstUserRegionId.toString), Work)
    userActivityPoisList should contain(
      (User("", "", secondUserMsisdn), secondUserSiteId, secondUserRegionId.toString), Home)
    userActivityPoisList should contain(
      (User("", "", firstUserMsisdn), secondUserSiteId, secondUserRegionId.toString), Home)
  }

  it should "group the pois by user" in new WithUserActivity {
    val userPoisList = userPois.collect.toSeq

    userPoisList.length should be(2)
    userPoisList should contain ((User("", "", firstUserMsisdn),
      List((Home, List((secondUserSiteId, secondUserRegionId.toString))),
        (Work, List((firstUserSiteId, firstUserRegionId.toString), (firstUserSiteId, firstUserRegionId.toString))))))
    userPoisList should contain(
      User("", "", secondUserMsisdn),
      List((Home, List((secondUserSiteId, secondUserRegionId.toString)))))
  }

  it should "return an RDD with user msisdn, poi type and its geometries" in new WithUserActivity {
    val broadcastCatalogue = sc.broadcast(btsCatalogue)
    val userPoisWithGeomsList = userActivity.userPoisWithGeoms(
      model, centroidsMapping, broadcastCatalogue).collect.toList

    userPoisWithGeomsList.length should be(3)
    userPoisWithGeomsList(1) should be(
      (User("", "", firstUserMsisdn), Work, Seq(egBts1.geom, egBts2.geom, egBts1.geom, egBts2.geom)))
    userPoisWithGeomsList(2) should be((User("", "", secondUserMsisdn), Home, Seq(egBts2.geom)))
  }

  it should "return the user Pois and their aggregated geoms" in new WithUserActivity {
    val broadcastCatalogue = sc.broadcast(btsCatalogue)
    val userPoisWithGeomsList =
      userActivity.userPoisWithAggregatedGeoms(model, centroidsMapping, broadcastCatalogue).collect.toList

    userPoisWithGeomsList.length should be(3)
    userPoisWithGeomsList(0) should be((User("", "", firstUserMsisdn), Home, homeGeom))
    userPoisWithGeomsList(1) should be((User("", "", firstUserMsisdn), Work, unionGeom))
    userPoisWithGeomsList(2) should be((User("", "", secondUserMsisdn), Home, homeGeom))
  }
}
