/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi.spark

import com.github.nscala_time.time.Imports._
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.scalatest.{ShouldMatchers, FlatSpec}

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

    val unionGeom = GeomUtils.parseWkt("POLYGON ((822146.1 3086196.3, 822121.1 3086166.2, 822027.5 3086078.3, 821923.7" +
      " 3086002.8, 821811.2 3085941, 821691.8 3085893.7, 821567.5 3085861.8, 821440.1 3085845.7, 821311.7 3085845.7," +
      " 821184.3 3085861.8, 821060 3085893.7, 820940.6 3085941, 820828.1 3086002.8, 820724.3 3086078.3, 820630.7" +
      " 3086166.2, 820548.8 3086265.1, 820480 3086373.5, 820425.4 3086489.7, 820385.7 3086611.8, 820361.6 3086737.9," +
      " 820353.6 3086866, 820361.6 3086994.1, 820385.7 3087120.2, 820425.4 3087242.3, 820480 3087358.5, 820548.8 " +
      "3087466.9, 820630.7 3087565.8, 820715.7 3087645.7, 820740.7 3087675.8, 820834.3 3087763.7, 820938.1 3087839.2," +
      " 821050.6 3087901, 821170 3087948.3, 821294.3 3087980.2, 821421.7 3087996.3, 821550.1 3087996.3, 821677.5 " +
      "3087980.2, 821801.8 3087948.3, 821921.2 3087901, 822033.7 3087839.2, 822137.5 3087763.7, 822231.1 3087675.8," +
      " 822313 3087576.9, 822381.8 3087468.5, 822436.4 3087352.3, 822476.1 3087230.2, 822500.2 3087104.1, 822508.2" +
      " 3086976, 822500.2 3086847.9, 822476.1 3086721.8, 822436.4 3086599.7, 822381.8 3086483.5, 822313 3086375.1, " +
      "822231.1 3086276.2, 822146.1 3086196.3))",Coordinates.SaudiArabiaUtmSrid)

    val homeGeom = GeomUtils.parseWkt("POLYGON ((822508.2 3086976, 822500.2 3086847.9, 822476.1 3086721.8, 822436.4 " +
      "3086599.7, 822381.8 3086483.5, 822313 3086375.1, 822231.1 3086276.2, 822137.5 3086188.3, 822033.7 3086112.8, " +
      "821921.2 3086051, 821801.8 3086003.7, 821677.5 3085971.8, 821550.1 3085955.7, 821421.7 3085955.7, 821294.3 " +
      "3085971.8, 821170 3086003.7, 821050.6 3086051, 820938.1 3086112.8, 820834.3 3086188.3, 820740.7 3086276.2, " +
      "820658.8 3086375.1, 820590 3086483.5, 820535.4 3086599.7, 820495.7 3086721.8, 820471.6 3086847.9, 820463.6 " +
      "3086976, 820471.6 3087104.1, 820495.7 3087230.2, 820535.4 3087352.3, 820590 3087468.5, 820658.8 3087576.9, " +
      "820740.7 3087675.8, 820834.3 3087763.7, 820938.1 3087839.2, 821050.6 3087901, 821170 3087948.3, 821294.3 " +
      "3087980.2, 821421.7 3087996.3, 821550.1 3087996.3, 821677.5 3087980.2, 821801.8 3087948.3, 821921.2 3087901, " +
      "822033.7 3087839.2, 822137.5 3087763.7, 822231.1 3087675.8, 822313 3087576.9, 822381.8 3087468.5, 822436.4 " +
      "3087352.3, 822476.1 3087230.2, 822500.2 3087104.1, 822508.2 3086976))", Coordinates.SaudiArabiaUtmSrid)

    val centroidsMapping = Map(0 -> Home, 1 -> Work, 2 -> TypeOne, 3 -> TypeTwo, 4 -> TypeThree, 5 -> TypeFour,
      6 -> TypeFive, 7 -> TypeSix)
    val pois = userActivity.pois(model, centroidsMapping)
    val userPois = userActivity.userPois(model, centroidsMapping)
  }

  "UserActivityPoiDsl" should "assign the centroids to each entry of the vector" in new WithUserActivity {
    val userActivityPoisList = pois.collect.toList

    userActivityPoisList.length should be(4)
    userActivityPoisList should contain (
      (User("", "", firstUserMsisdn), firstUserSiteId, firstUserRegionId.toString), Work)
    userActivityPoisList should contain (
      (User("", "", secondUserMsisdn), secondUserSiteId, secondUserRegionId.toString), Home)
    userActivityPoisList should contain (
      (User("", "", firstUserMsisdn), secondUserSiteId, secondUserRegionId.toString), Home)
  }

  it should "group the pois by user" in new WithUserActivity {
    val userPoisList = userPois.collect.toSeq

    userPoisList.length should be(2)
    userPoisList should contain ((User("", "", firstUserMsisdn),
      List((Work, List((firstUserSiteId, firstUserRegionId.toString), (firstUserSiteId, firstUserRegionId.toString))),
        (Home, List((secondUserSiteId, secondUserRegionId.toString))))))
    userPoisList should contain (
      User("", "", secondUserMsisdn),
      List((Home, List((secondUserSiteId, secondUserRegionId.toString)))))
  }

  it should "return an RDD with user msisdn, poi type and its geometries" in new WithUserActivity {
    val broadcastCatalogue = sc.broadcast(btsCatalogue)
    val userPoisWithGeomsList = userActivity.userPoisWithGeoms(
      model, centroidsMapping, broadcastCatalogue).collect.toList

    userPoisWithGeomsList.length should be(3)
    userPoisWithGeomsList should contain theSameElementsAs (List(
      (User("", "", firstUserMsisdn), Work, Seq(egBts1.geom, egBts2.geom, egBts1.geom, egBts2.geom)),
      (User("", "", firstUserMsisdn), Home, Seq(egBts2.geom)),
      (User("", "", secondUserMsisdn), Home, Seq(egBts2.geom))))
  }

  it should "return the user Pois and their aggregated geoms" in new WithUserActivity {
    val broadcastCatalogue = sc.broadcast(btsCatalogue)
    val poi = userActivity.userPoisWithAggregatedGeoms(model, centroidsMapping, broadcastCatalogue).collect.toList

    poi.length should be(3)
    poi should contain theSameElementsAs (List(
      Poi(User("", "", firstUserMsisdn), Work, GeomUtils.wkt(unionGeom)),
      Poi(User("", "", firstUserMsisdn), Home, GeomUtils.wkt(homeGeom)),
      Poi(User("", "", secondUserMsisdn), Home, GeomUtils.wkt(homeGeom))))
  }
}
