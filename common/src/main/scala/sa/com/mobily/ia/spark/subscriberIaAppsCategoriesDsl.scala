/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import sa.com.mobily.ia.SubscriberIaAppsCategories
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}

class SubscriberIaAppsCategoriesReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedSubscriberIaAppsCategories: RDD[ParsedItem[SubscriberIaAppsCategories]] =
    SparkParser.fromCsv[SubscriberIaAppsCategories](self)

  def toSubscriberIaAppsCategories: RDD[SubscriberIaAppsCategories] = toParsedSubscriberIaAppsCategories.values

  def toSubscriberIaAppsCategoriesErrors: RDD[ParsingError] = toParsedSubscriberIaAppsCategories.errors
}

class SubscriberIaAppsCategoriesRowReader(self: RDD[Row]) {

  def toSubscriberIaAppsCategories: RDD[SubscriberIaAppsCategories] =
    SparkParser.fromRow[SubscriberIaAppsCategories](self)
}

class SubscriberIaAppsCategoriesWriter(self: RDD[SubscriberIaAppsCategories]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[SubscriberIaAppsCategories](self, path)
}

trait SubscriberIaAppsCategoriesDsl {

  implicit def subscriberAppsCategoriesReader(csv: RDD[String]): SubscriberIaAppsCategoriesReader =
    new SubscriberIaAppsCategoriesReader(csv)

  implicit def subscriberIaAppsCategoriesRowReader(self: RDD[Row]): SubscriberIaAppsCategoriesRowReader =
    new SubscriberIaAppsCategoriesRowReader(self)

  implicit def subscriberIaAppsCategoriesWriter(
      subscriberIaAppsCategories: RDD[SubscriberIaAppsCategories]): SubscriberIaAppsCategoriesWriter =
    new SubscriberIaAppsCategoriesWriter(subscriberIaAppsCategories)
}

object SubscriberIaAppsCategoriesDsl extends SubscriberIaAppsCategoriesDsl with ParsedItemsDsl
