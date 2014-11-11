/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.ia.SubscriberIaDomains
import sa.com.mobily.parsing.{ParsingError, ParsedItem}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser}

class SubscriberIaDomainsReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedSubscriberIaDomains: RDD[ParsedItem[SubscriberIaDomains]] =
    SparkParser.fromCsv[SubscriberIaDomains](self)

  def toSubscriberIaDomains: RDD[SubscriberIaDomains] = toParsedSubscriberIaDomains.values

  def toSubscriberIaDomainsErrors: RDD[ParsingError] = toParsedSubscriberIaDomains.errors
}

trait SubscriberIaDomainsDsl {

  implicit def subscriberDomainsReader(csv: RDD[String]): SubscriberIaDomainsReader = new SubscriberIaDomainsReader(csv)
}

object SubscriberIaDomainsDsl extends SubscriberIaDomainsDsl with ParsedItemsDsl
