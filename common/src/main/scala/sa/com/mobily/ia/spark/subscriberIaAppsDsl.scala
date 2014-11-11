/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.ia.SubscriberIaApps
import sa.com.mobily.parsing.{ParsingError, ParsedItem}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser}

class SubscriberIaAppsReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedSubscriberIaApps: RDD[ParsedItem[SubscriberIaApps]] = SparkParser.fromCsv[SubscriberIaApps](self)

  def toSubscriberIaApps: RDD[SubscriberIaApps] = toParsedSubscriberIaApps.values

  def toSubscriberIaAppsErrors: RDD[ParsingError] = toParsedSubscriberIaApps.errors
}

trait SubscriberIaAppsDsl {

  implicit def subscriberAppsReader(csv: RDD[String]): SubscriberIaAppsReader = new SubscriberIaAppsReader(csv)
}

object SubscriberIaAppsDsl extends SubscriberIaAppsDsl with ParsedItemsDsl
