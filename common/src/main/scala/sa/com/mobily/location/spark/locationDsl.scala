/*
 * TODO: License goes here!
 */

package sa.com.mobily.location.spark

import org.apache.spark.rdd.RDD

import sa.com.mobily.location.Location
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser}
import sa.com.mobily.parsing.{ParsedItem, ParsingError}

import scala.language.implicitConversions

class LocationReader(self: RDD[String]) {

  import sa.com.mobily.parsing.spark.ParsedItemsDsl._

  def toParsedLocation: RDD[ParsedItem[Location]] = SparkParser.fromCsv[Location](self)

  def toLocation: RDD[Location] = toParsedLocation.values

  def toLocationErrors: RDD[ParsingError] = toParsedLocation.errors
}

trait LocationDsl {

  implicit def locationReader(csv: RDD[String]): LocationReader = new LocationReader(csv)
}

object LocationDsl extends LocationDsl with ParsedItemsDsl
