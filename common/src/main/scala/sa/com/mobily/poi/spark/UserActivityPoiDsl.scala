/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi.spark

import scala.language.implicitConversions

import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import sa.com.mobily.poi.PoiType

class UserActivityPoiFunctions(self: RDD[((Long, String, Long), Vector)]) {

  def pois(model: KMeansModel, centroidMapping: Map[Int, PoiType]): RDD[((Long, String, Long), PoiType)] =
    self.map(element => (element._1, centroidMapping(model.predict(element._2))))
}

trait UserActivityPoiDsl {

  implicit def activityPoiFunctions(activity: RDD[((Long, String, Long), Vector)]): UserActivityPoiFunctions =
    new UserActivityPoiFunctions(activity)
}

object UserActivityPoiDsl extends UserActivityPoiDsl with UserPhoneCallsDsl
