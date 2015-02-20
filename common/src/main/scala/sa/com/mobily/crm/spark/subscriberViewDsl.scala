/*
 * TODO: License goes here!
 */

package sa.com.mobily.crm.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import sa.com.mobily.crm._
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}

class SubscriberViewCsvReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedSubscriberView: RDD[ParsedItem[SubscriberView]] = SparkParser.fromCsv[SubscriberView](self)

  def toSubscriberView: RDD[SubscriberView] = toParsedSubscriberView.values

  def toSubscriberViewErrors: RDD[ParsingError] = toParsedSubscriberView.errors
}

class SubscriberViewWriter(self: RDD[SubscriberView]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[SubscriberView](self, path)
}

class SubscriberViewRowReader(self: RDD[Row]) {

  def toSubscriberView: RDD[SubscriberView] = SparkParser.fromRow[SubscriberView](self)
}

trait SubscriberViewDsl {

  implicit def subscriberViewCsvReader(csv: RDD[String]): SubscriberViewCsvReader =
    new SubscriberViewCsvReader(csv)

  implicit def subscriberViewWriter(self: RDD[SubscriberView]): SubscriberViewWriter =
    new SubscriberViewWriter(self)

  implicit def subscriberViewRowReader(self: RDD[Row]): SubscriberViewRowReader =
    new SubscriberViewRowReader(self)
}

object SubscriberViewDsl extends SubscriberViewDsl with ParsedItemsDsl
