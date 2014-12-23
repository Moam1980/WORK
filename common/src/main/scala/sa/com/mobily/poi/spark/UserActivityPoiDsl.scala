/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi.spark

import scala.language.implicitConversions

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.rdd.RDD

import sa.com.mobily.cell.EgBts
import sa.com.mobily.poi.{PoiType, UserActivity, UserActivityPoi}
import sa.com.mobily.user.User

class UserActivityPoiFunctions(self: RDD[UserActivity]) {

  def pois(model: KMeansModel, centroidMapping: Map[Int, PoiType]): RDD[((User, String, Short), PoiType)] =
    self.map(userActivity =>
      ((userActivity.user, userActivity.siteId, userActivity.regionId),
        centroidMapping(model.predict(userActivity.activityVector))))

  def userPois(implicit model: KMeansModel, centroidMapping: Map[Int, PoiType]):
      RDD[(User, Seq[(PoiType, Iterable[(String, Short)])])] = {
    val poisPerUser = pois(model, centroidMapping).map(usrBtsPoi =>
      (usrBtsPoi._1._1, (usrBtsPoi._2, (usrBtsPoi._1._2, usrBtsPoi._1._3.toShort)))).groupByKey
    poisPerUser.map(userPoiBts => {
      val user = userPoiBts._1
      val pois = userPoiBts._2.groupBy(_._1).mapValues(_.map(_._2)).toSeq
      (user, pois)
    })
  }

  def userPoisWithGeoms(
      implicit model: KMeansModel,
      centroidMapping: Map[Int, PoiType],
      btsCatalogue: Broadcast[Map[(String, Short), Iterable[EgBts]]]): RDD[(User, PoiType, Iterable[Geometry])] = {
    for (userPois <- userPois(model, centroidMapping); poi <- userPois._2)
    yield (userPois._1, poi._1, UserActivityPoi.findGeometries(poi._2, btsCatalogue.value))
  }

  def userPoisWithAggregatedGeoms(
      aggregateGeometries: Iterable[Geometry] => Geometry = itGeoms => itGeoms.reduce(_.intersection(_)))
      (implicit model: KMeansModel,
      centroidMapping: Map[Int, PoiType],
      btsCatalogue: Broadcast[Map[(String, Short), Iterable[EgBts]]]): RDD[(User, PoiType, Geometry)] = {
    userPoisWithGeoms(model, centroidMapping, btsCatalogue).map(userPoi =>
      (userPoi._1, userPoi._2, aggregateGeometries(userPoi._3)))
  }
}

trait UserActivityPoiDsl {

  implicit def activityPoiFunctions(activity: RDD[UserActivity]): UserActivityPoiFunctions =
    new UserActivityPoiFunctions(activity)
}

object UserActivityPoiDsl extends UserActivityPoiDsl with UserActivityCdrDsl
