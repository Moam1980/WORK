/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.ia.SubscriberIaSearches
import sa.com.mobily.parsing.{ParsingError, ParsedItem}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser}

class SubscriberIaSearchesReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedSubscriberIaSearches: RDD[ParsedItem[SubscriberIaSearches]] =
    SparkParser.fromCsv[SubscriberIaSearches](self)

  def toSubscriberIaSearches: RDD[SubscriberIaSearches] = toParsedSubscriberIaSearches.values

  def toSubscriberIaSearchesErrors: RDD[ParsingError] = toParsedSubscriberIaSearches.errors
}

trait SubscriberIaSearchesDsl {

  implicit def subscriberSearchesReader(csv: RDD[String]): SubscriberIaSearchesReader =
    new SubscriberIaSearchesReader(csv)
}

object SubscriberIaSearchesDsl extends SubscriberIaSearchesDsl with ParsedItemsDsl
