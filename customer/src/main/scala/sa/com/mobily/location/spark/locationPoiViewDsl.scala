/*
 * TODO: License goes here!
 */

package sa.com.mobily.location.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import sa.com.mobily.location.{LocationPoiView, UserPoiProfiling}
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}
import sa.com.mobily.poi.ProfilingPoiType

class LocationPoiViewCsvReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedLocationPoiView: RDD[ParsedItem[LocationPoiView]] = SparkParser.fromCsv[LocationPoiView](self)

  def toLocationPoiView: RDD[LocationPoiView] = toParsedLocationPoiView.values

  def toLocationPoiViewErrors: RDD[ParsingError] = toParsedLocationPoiView.errors
}

class LocationPoiViewWriter(self: RDD[LocationPoiView]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[LocationPoiView](self, path)
}

class LocationPoiViewRowReader(self: RDD[Row]) {

  def toLocationPoiView: RDD[LocationPoiView] = SparkParser.fromRow[LocationPoiView](self)
}

class LocationPoiViewFunctions(self: RDD[LocationPoiView]) {

  def toUserPoiProfiling: RDD[UserPoiProfiling] =
    self.map(lpv => (lpv.imsi, lpv.poiType)).distinct.groupByKey.map(uP =>
      UserPoiProfiling(uP._1, ProfilingPoiType.parseUserPoiType(uP._2.toList)))
}

trait LocationPoiViewDsl {

  implicit def locationPoiViewReader(csv: RDD[String]): LocationPoiViewCsvReader = new LocationPoiViewCsvReader(csv)

  implicit def locationPoiViewWriter(self: RDD[LocationPoiView]): LocationPoiViewWriter =
    new LocationPoiViewWriter(self)

  implicit def locationPoiViewRowReader(self: RDD[Row]): LocationPoiViewRowReader = new LocationPoiViewRowReader(self)

  implicit def locationPoiViewFunctions(locationPoiViews: RDD[LocationPoiView]): LocationPoiViewFunctions =
    new LocationPoiViewFunctions(locationPoiViews)
}

object LocationPoiViewDsl extends LocationPoiViewDsl with ParsedItemsDsl
