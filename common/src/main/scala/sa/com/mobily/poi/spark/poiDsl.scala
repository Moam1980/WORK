/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import sa.com.mobily.geometry.GeomUtils
import sa.com.mobily.parsing.spark.{SparkParser, SparkWriter}
import sa.com.mobily.poi._
import sa.com.mobily.user.User
import sa.com.mobily.utils.Stats

class PoiRowReader(self: RDD[Row]) {

  def toPoi: RDD[Poi] = SparkParser.fromRow[Poi](self)
}

class PoiWriter(self: RDD[Poi]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[Poi](self, path)
}

class PoiFunctions(self: RDD[Poi]) {

  def perTypeCombination: Map[Seq[PoiType], Long] = {
    val userPoiTypes = self.map(poi => (poi.user, poi.poiType)).groupByKey
    userPoiTypes.map(userPoiTypes => (userPoiTypes._2.toSeq.sortBy(_.toString), userPoiTypes._1)).countByKey.toMap
  }

  def distanceSubpolygons: RDD[(PoiType, Stats)] = {
    val poisWithSubPolygons = self.filter(_.geometry.getNumGeometries > 1).cache
    val distancePoisSubPolygons = poisWithSubPolygons.map(poi =>
      (poi.poiType, GeomUtils.distanceSubpolygons(poi.geometry)))
    poisWithSubPolygons.unpersist(false)

    distancePoisSubPolygons
  }

  def distancePerTypeCombination: Map[Seq[PoiType], Stats] = {
    val userPoisDistance = self.keyBy(_.user).groupByKey.filter(uP => uP._2.size > 1)
    val distanceByType = userPoisDistance.flatMap(userPois => {
      val pois = userPois._2.toArray
      val cartesianProduct = { for (i <- 0 until pois.size; j <- (i + 1) until pois.size) yield (pois(i), pois(j)) }
      cartesianProduct.map(c =>
        (Seq(c._1.poiType, c._2.poiType).sortBy(_.toString), c._1.geometry.distance(c._2.geometry)))
    })

    distanceByType.groupByKey.map(typeDistance =>
      (typeDistance._1, Stats(typeDistance._2.toArray))).collect.toMap
  }

  def poiMetrics: PoiMetrics  = {
    val total = self.count
    val usersCounter = self.map(_.user).distinct.count

    val distanceSubpolygonsPerType = distanceSubpolygons.groupByKey.map(poiTypeStats =>
      (poiTypeStats._1, Stats.aggByMean(poiTypeStats._2.toArray))).collect.toMap

    PoiMetrics(
      numUsers = usersCounter,
      numPois = total,
      numUsersPerTypeCombination = perTypeCombination,
      distancePoisPerTypeCombination = distancePerTypeCombination,
      distancePoisSubPolygonsStats = distanceSubpolygonsPerType)
  }

  def filterByUsers(users: RDD[User]): RDD[Poi] = {
    users.keyBy(u => u.imsi).join(self.keyBy(p => p.user.imsi)).map(userPoi => userPoi._2._2)
  }
}

trait PoiDsl {

  implicit def poiRowReader(self: RDD[Row]): PoiRowReader = new PoiRowReader(self)

  implicit def poiWriter(pois: RDD[Poi]): PoiWriter = new PoiWriter(pois)

  implicit def poiFunctions(pois: RDD[Poi]): PoiFunctions = new PoiFunctions(pois)
}

object PoiDsl extends PoiDsl
