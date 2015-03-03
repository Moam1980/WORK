/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import sa.com.mobily.ia.SubscriberIaDomains
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}

class SubscriberIaDomainsReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedSubscriberIaDomains: RDD[ParsedItem[SubscriberIaDomains]] =
    SparkParser.fromCsv[SubscriberIaDomains](self)

  def toSubscriberIaDomains: RDD[SubscriberIaDomains] = toParsedSubscriberIaDomains.values

  def toSubscriberIaDomainsErrors: RDD[ParsingError] = toParsedSubscriberIaDomains.errors
}

class SubscriberIaDomainsRowReader(self: RDD[Row]) {

  def toSubscriberIaDomains: RDD[SubscriberIaDomains] =
    SparkParser.fromRow[SubscriberIaDomains](self)
}

class SubscriberIaDomainsWriter(self: RDD[SubscriberIaDomains]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[SubscriberIaDomains](self, path)
}

trait SubscriberIaDomainsDsl {

  implicit def subscriberDomainsReader(csv: RDD[String]): SubscriberIaDomainsReader = new SubscriberIaDomainsReader(csv)

  implicit def subscriberIaDomainsRowReader(self: RDD[Row]): SubscriberIaDomainsRowReader =
    new SubscriberIaDomainsRowReader(self)

  implicit def subscriberIaDomainsWriter(subscriberIaDomains: RDD[SubscriberIaDomains]): SubscriberIaDomainsWriter =
    new SubscriberIaDomainsWriter(subscriberIaDomains)
}

object SubscriberIaDomainsDsl extends SubscriberIaDomainsDsl with ParsedItemsDsl
