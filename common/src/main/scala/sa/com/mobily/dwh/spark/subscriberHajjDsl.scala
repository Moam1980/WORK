/*
 * TODO: License goes here!
 */

package sa.com.mobily.dwh.spark

import org.apache.spark.rdd.RDD

import sa.com.mobily.dwh.SubscriberHajj
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkCsvParser}

import scala.language.implicitConversions

class SubscriberHajjReader(self: RDD[String]) {

  import sa.com.mobily.parsing.spark.ParsedItemsDsl._

  def toParsedSubscriberHajj: RDD[ParsedItem[SubscriberHajj]] = SparkCsvParser.fromCsv[SubscriberHajj](self)

  def toSubscriberHajj: RDD[SubscriberHajj] = toParsedSubscriberHajj.values

  def toSubscriberHajjErrors: RDD[ParsingError] = toParsedSubscriberHajj.errors
}

trait SubscriberHajjDsl {

  implicit def subscriberDwhReader(csv: RDD[String]): SubscriberHajjReader = new SubscriberHajjReader(csv)
}

object SubscriberHajjDsl extends SubscriberHajjDsl with ParsedItemsDsl
