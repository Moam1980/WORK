/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.ia.SubscriberIaAppsCategories
import sa.com.mobily.parsing.{ParsingError, ParsedItem}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkCsvParser}

class SubscriberIaAppsCategoriesReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedSubscriberIaAppsCategories: RDD[ParsedItem[SubscriberIaAppsCategories]] =
    SparkCsvParser.fromCsv[SubscriberIaAppsCategories](self)

  def toSubscriberIaAppsCategories: RDD[SubscriberIaAppsCategories] = toParsedSubscriberIaAppsCategories.values

  def toSubscriberIaAppsCategoriesErrors: RDD[ParsingError] = toParsedSubscriberIaAppsCategories.errors
}

trait SubscriberIaAppsCategoriesDsl {

  implicit def subscriberAppsCategoriesReader(csv: RDD[String]): SubscriberIaAppsCategoriesReader =
    new SubscriberIaAppsCategoriesReader(csv)
}

object SubscriberIaAppsCategoriesDsl extends SubscriberIaAppsCategoriesDsl with ParsedItemsDsl
