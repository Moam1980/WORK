/*
 * TODO: License goes here!
 */

package sa.com.mobily.location.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import sa.com.mobily.location.LocationPointView
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}

class LocationPointViewCsvReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedLocationPointView: RDD[ParsedItem[LocationPointView]] = SparkParser.fromCsv[LocationPointView](self)

  def toLocationPointView: RDD[LocationPointView] = toParsedLocationPointView.values

  def toLocationPointViewErrors: RDD[ParsingError] = toParsedLocationPointView.errors
}

class LocationPointViewWriter(self: RDD[LocationPointView]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[LocationPointView](self, path)
}

class LocationPointViewRowReader(self: RDD[Row]) {

  def toLocationPointView: RDD[LocationPointView] = SparkParser.fromRow[LocationPointView](self)
}

trait LocationPointViewDsl {

  implicit def locationPointViewReader(csv: RDD[String]): LocationPointViewCsvReader =
    new LocationPointViewCsvReader(csv)

  implicit def locationPointViewWriter(self: RDD[LocationPointView]): LocationPointViewWriter =
    new LocationPointViewWriter(self)

  implicit def locationPointViewRowReader(self: RDD[Row]): LocationPointViewRowReader =
    new LocationPointViewRowReader(self)
}

object LocationPointViewDsl extends LocationPointViewDsl with ParsedItemsDsl
