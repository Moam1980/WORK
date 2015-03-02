/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import sa.com.mobily.ia.SubscriberIaSearches
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}

class SubscriberIaSearchesReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedSubscriberIaSearches: RDD[ParsedItem[SubscriberIaSearches]] =
    SparkParser.fromCsv[SubscriberIaSearches](self)

  def toSubscriberIaSearches: RDD[SubscriberIaSearches] = toParsedSubscriberIaSearches.values

  def toSubscriberIaSearchesErrors: RDD[ParsingError] = toParsedSubscriberIaSearches.errors
}

class SubscriberIaSearchesRowReader(self: RDD[Row]) {

  def toSubscriberIaSearches: RDD[SubscriberIaSearches] = SparkParser.fromRow[SubscriberIaSearches](self)
}

class SubscriberIaSearchesWriter(self: RDD[SubscriberIaSearches]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[SubscriberIaSearches](self, path)
}

trait SubscriberIaSearchesDsl {

  implicit def subscriberSearchesReader(csv: RDD[String]): SubscriberIaSearchesReader =
    new SubscriberIaSearchesReader(csv)

  implicit def subscriberIaSearchesRowReader(self: RDD[Row]): SubscriberIaSearchesRowReader =
    new SubscriberIaSearchesRowReader(self)

  implicit def subscriberIaSearchesWriter(subscriberIaSearches: RDD[SubscriberIaSearches]): SubscriberIaSearchesWriter =
    new SubscriberIaSearchesWriter(subscriberIaSearches)
}

object SubscriberIaSearchesDsl extends SubscriberIaSearchesDsl with ParsedItemsDsl
