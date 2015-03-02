/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import sa.com.mobily.ia.SubscriberIaApps
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}

class SubscriberIaAppsReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedSubscriberIaApps: RDD[ParsedItem[SubscriberIaApps]] = SparkParser.fromCsv[SubscriberIaApps](self)

  def toSubscriberIaApps: RDD[SubscriberIaApps] = toParsedSubscriberIaApps.values

  def toSubscriberIaAppsErrors: RDD[ParsingError] = toParsedSubscriberIaApps.errors
}

class SubscriberIaAppsRowReader(self: RDD[Row]) {

  def toSubscriberIaApps: RDD[SubscriberIaApps] = SparkParser.fromRow[SubscriberIaApps](self)
}

class SubscriberIaAppsWriter(self: RDD[SubscriberIaApps]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[SubscriberIaApps](self, path)
}

trait SubscriberIaAppsDsl {

  implicit def subscriberAppsReader(csv: RDD[String]): SubscriberIaAppsReader = new SubscriberIaAppsReader(csv)

  implicit def subscriberIaAppsRowReader(self: RDD[Row]): SubscriberIaAppsRowReader =
    new SubscriberIaAppsRowReader(self)

  implicit def subscriberIaAppsWriter(subscriberIaApps: RDD[SubscriberIaApps]): SubscriberIaAppsWriter =
    new SubscriberIaAppsWriter(subscriberIaApps)
}

object SubscriberIaAppsDsl extends SubscriberIaAppsDsl with ParsedItemsDsl
