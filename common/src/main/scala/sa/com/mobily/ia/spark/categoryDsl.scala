/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import sa.com.mobily.ia.Category
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}

class CategoryReader(self: RDD[String]) {

  import sa.com.mobily.parsing.spark.ParsedItemsDsl._

  def toParsedCategory: RDD[ParsedItem[Category]] = SparkParser.fromCsv[Category](self)

  def toCategory: RDD[Category] = toParsedCategory.values

  def toCategoryErrors: RDD[ParsingError] = toParsedCategory.errors
}

class CategoryRowReader(self: RDD[Row]) {

  def toCategory: RDD[Category] = SparkParser.fromRow[Category](self)
}

class CategoryWriter(self: RDD[Category]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[Category](self, path)
}

trait CategoryDsl {

  implicit def categoryReader(csv: RDD[String]): CategoryReader = new CategoryReader(csv)

  implicit def categoryRowReader(self: RDD[Row]): CategoryRowReader = new CategoryRowReader(self)

  implicit def categoryWriter(categories: RDD[Category]): CategoryWriter = new CategoryWriter(categories)
}

object CategoryDsl extends CategoryDsl with ParsedItemsDsl
