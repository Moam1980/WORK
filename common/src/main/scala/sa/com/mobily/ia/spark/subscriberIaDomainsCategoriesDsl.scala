/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.ia.SubscriberIaDomainsCategories
import sa.com.mobily.parsing.{ParsingError, ParsedItem}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkCsvParser}

class SubscriberIaDomainsCategoriesReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedSubscriberIaDomainsCategories: RDD[ParsedItem[SubscriberIaDomainsCategories]] =
    SparkCsvParser.fromCsv[SubscriberIaDomainsCategories](self)

  def toSubscriberIaDomainsCategories: RDD[SubscriberIaDomainsCategories] = toParsedSubscriberIaDomainsCategories.values

  def toSubscriberIaDomainsCategoriesErrors: RDD[ParsingError] = toParsedSubscriberIaDomainsCategories.errors
}

trait SubscriberIaDomainsCategoriesDsl {

  implicit def subscriberDomainsCategoriesReader(csv: RDD[String]): SubscriberIaDomainsCategoriesReader =
    new SubscriberIaDomainsCategoriesReader(csv)
}

object SubscriberIaDomainsCategoriesDsl extends SubscriberIaDomainsCategoriesDsl with ParsedItemsDsl
