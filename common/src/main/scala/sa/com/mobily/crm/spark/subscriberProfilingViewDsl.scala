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

class SubscriberProfilingViewCsvReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedSubscriberProfilingView: RDD[ParsedItem[SubscriberProfilingView]] =
    SparkParser.fromCsv[SubscriberProfilingView](self)

  def toSubscriberProfilingView: RDD[SubscriberProfilingView] = toParsedSubscriberProfilingView.values

  def toSubscriberProfilingViewErrors: RDD[ParsingError] = toParsedSubscriberProfilingView.errors
}

class SubscriberProfilingViewWriter(self: RDD[SubscriberProfilingView]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[SubscriberProfilingView](self, path)
}

class SubscriberProfilingViewRowReader(self: RDD[Row]) {

  def toSubscriberProfilingView: RDD[SubscriberProfilingView] = SparkParser.fromRow[SubscriberProfilingView](self)
}

trait SubscriberProfilingViewDsl {

  implicit def subscriberProfilingViewCsvReader(csv: RDD[String]): SubscriberProfilingViewCsvReader =
    new SubscriberProfilingViewCsvReader(csv)

  implicit def subscriberProfilingViewWriter(self: RDD[SubscriberProfilingView]): SubscriberProfilingViewWriter =
    new SubscriberProfilingViewWriter(self)

  implicit def subscriberProfilingViewRowReader(self: RDD[Row]): SubscriberProfilingViewRowReader =
    new SubscriberProfilingViewRowReader(self)
}

object SubscriberProfilingViewDsl extends SubscriberProfilingViewDsl with ParsedItemsDsl
