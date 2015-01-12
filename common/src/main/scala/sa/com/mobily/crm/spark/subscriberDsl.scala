/*
 * TODO: License goes here!
 */

package sa.com.mobily.crm.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.crm.Subscriber
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser}

class SubscriberReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedSubscriber: RDD[ParsedItem[Subscriber]] = SparkParser.fromCsv[Subscriber](self)

  def toSubscriber: RDD[Subscriber] = toParsedSubscriber.values

  def toSubscriberErrors: RDD[ParsingError] = toParsedSubscriber.errors
}

trait SubscriberDsl {

  implicit def customerSubscriberReader(csv: RDD[String]): SubscriberReader = new SubscriberReader(csv)
}

object SubscriberDsl extends SubscriberDsl with ParsedItemsDsl
