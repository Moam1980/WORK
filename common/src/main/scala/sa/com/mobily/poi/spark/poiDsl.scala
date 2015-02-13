/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import sa.com.mobily.parsing.spark.{SparkParser, SparkWriter}
import sa.com.mobily.poi.Poi

class PoiRowReader(self: RDD[Row]) {

  def toPoi: RDD[Poi] = SparkParser.fromRow[Poi](self)
}

class PoiWriter(self: RDD[Poi]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[Poi](self, path)
}

trait PoiDsl {

  implicit def poiRowReader(self: RDD[Row]): PoiRowReader = new PoiRowReader(self)

  implicit def poiWriter(pois: RDD[Poi]): PoiWriter = new PoiWriter(pois)
}

object PoiDsl extends PoiDsl
