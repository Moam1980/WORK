/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import sa.com.mobily.ia.SubscriberIaDomainsCategories
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}

class SubscriberIaDomainsCategoriesReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedSubscriberIaDomainsCategories: RDD[ParsedItem[SubscriberIaDomainsCategories]] =
    SparkParser.fromCsv[SubscriberIaDomainsCategories](self)

  def toSubscriberIaDomainsCategories: RDD[SubscriberIaDomainsCategories] = toParsedSubscriberIaDomainsCategories.values

  def toSubscriberIaDomainsCategoriesErrors: RDD[ParsingError] = toParsedSubscriberIaDomainsCategories.errors
}

class SubscriberIaDomainsCategoriesRowReader(self: RDD[Row]) {

  def toSubscriberIaDomainsCategories: RDD[SubscriberIaDomainsCategories] =
    SparkParser.fromRow[SubscriberIaDomainsCategories](self)
}

class SubscriberIaDomainsCategoriesWriter(self: RDD[SubscriberIaDomainsCategories]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[SubscriberIaDomainsCategories](self, path)
}

trait SubscriberIaDomainsCategoriesDsl {

  implicit def subscriberDomainsCategoriesReader(csv: RDD[String]): SubscriberIaDomainsCategoriesReader =
    new SubscriberIaDomainsCategoriesReader(csv)

  implicit def subscriberIaDomainsCategoriesRowReader(self: RDD[Row]): SubscriberIaDomainsCategoriesRowReader =
    new SubscriberIaDomainsCategoriesRowReader(self)

  implicit def subscriberIaDomainsCategoriesWriter(
    subscriberIaDomainsCategories: RDD[SubscriberIaDomainsCategories]): SubscriberIaDomainsCategoriesWriter =
    new SubscriberIaDomainsCategoriesWriter(subscriberIaDomainsCategories)
}

object SubscriberIaDomainsCategoriesDsl extends SubscriberIaDomainsCategoriesDsl with ParsedItemsDsl
