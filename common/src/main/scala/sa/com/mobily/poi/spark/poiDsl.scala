/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi.spark

import scala.language.implicitConversions

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import sa.com.mobily.geometry.GeomUtils
import sa.com.mobily.parsing.spark.{SparkParser, SparkWriter}
import sa.com.mobily.poi.{LocationPoiMetrics, PoiType, Poi}

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

  def locationPoiMetrics(locationGeom: Geometry) : LocationPoiMetrics  = {
    val total = self.count
    val usersCounter = self.map(_.user).distinct.count
    val poisType = perTypeCombination
    val intersectionRatio = self.map(p => GeomUtils.intersectionRatio(p.geometry, locationGeom)).cache
    val mean = intersectionRatio.mean
    val stDev = intersectionRatio.stdev
    val min = intersectionRatio.min
    val max = intersectionRatio.max
    intersectionRatio.unpersist(false)
    LocationPoiMetrics(mean, stDev, max, min, usersCounter, total, poisType)
  }
}

trait PoiDsl {

  implicit def poiRowReader(self: RDD[Row]): PoiRowReader = new PoiRowReader(self)

  implicit def poiWriter(pois: RDD[Poi]): PoiWriter = new PoiWriter(pois)

  implicit def poiFunctions(pois: RDD[Poi]): PoiFunctions = new PoiFunctions(pois)
}

object PoiDsl extends PoiDsl
