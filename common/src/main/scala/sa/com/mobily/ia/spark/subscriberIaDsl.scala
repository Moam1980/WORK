/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import sa.com.mobily.ia.SubscriberIa
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}

class SubscriberIaReader(self: RDD[String]) {

  import sa.com.mobily.parsing.spark.ParsedItemsDsl._

  def toParsedSubscriberIa: RDD[ParsedItem[SubscriberIa]] = SparkParser.fromCsv[SubscriberIa](self)

  def toSubscriberIa: RDD[SubscriberIa] = toParsedSubscriberIa.values

  def toSubscriberIaErrors: RDD[ParsingError] = toParsedSubscriberIa.errors
}

class SubscriberIaRowReader(self: RDD[Row]) {

  def toSubscriberIa: RDD[SubscriberIa] = SparkParser.fromRow[SubscriberIa](self)
}

class SubscriberIaWriter(self: RDD[SubscriberIa]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[SubscriberIa](self, path)
}

trait SubscriberIaDsl {

  implicit def subscriberReader(csv: RDD[String]): SubscriberIaReader = new SubscriberIaReader(csv)

  implicit def subscriberIaRowReader(self: RDD[Row]): SubscriberIaRowReader = new SubscriberIaRowReader(self)

  implicit def subscriberIaWriter(subscriberIa: RDD[SubscriberIa]): SubscriberIaWriter =
    new SubscriberIaWriter(subscriberIa)
}

object SubscriberIaDsl extends SubscriberIaDsl with ParsedItemsDsl
