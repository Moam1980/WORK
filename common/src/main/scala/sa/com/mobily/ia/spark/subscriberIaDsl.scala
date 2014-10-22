/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import org.apache.spark.rdd.RDD
import sa.com.mobily.ia.SubscriberIa
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkCsvParser}
import sa.com.mobily.parsing.{ParsedItem, ParsingError}

import scala.language.implicitConversions

class SubscriberIaReader(self: RDD[String]) {

  import sa.com.mobily.parsing.spark.ParsedItemsDsl._

  def toParsedSubscriberIa: RDD[ParsedItem[SubscriberIa]] = SparkCsvParser.fromCsv[SubscriberIa](self)

  def toSubscriberIa: RDD[SubscriberIa] = toParsedSubscriberIa.values

  def toSubscriberIaErrors: RDD[ParsingError] = toParsedSubscriberIa.errors
}

trait SubscriberIaDsl {

  implicit def subscriberReader(csv: RDD[String]): SubscriberIaReader = new SubscriberIaReader(csv)
}

object SubscriberIaDsl extends SubscriberIaDsl with ParsedItemsDsl
