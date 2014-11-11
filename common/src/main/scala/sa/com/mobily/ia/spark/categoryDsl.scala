/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import org.apache.spark.rdd.RDD
import sa.com.mobily.ia.Category
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser}
import sa.com.mobily.parsing.{ParsedItem, ParsingError}

import scala.language.implicitConversions

class CategoryReader(self: RDD[String]) {

  import sa.com.mobily.parsing.spark.ParsedItemsDsl._

  def toParsedCategory: RDD[ParsedItem[Category]] = SparkParser.fromCsv[Category](self)

  def toCategory: RDD[Category] = toParsedCategory.values

  def toCategoryErrors: RDD[ParsingError] = toParsedCategory.errors
}

trait CategoryDsl {

  implicit def categoryReader(csv: RDD[String]): CategoryReader = new CategoryReader(csv)
}

object CategoryDsl extends CategoryDsl with ParsedItemsDsl
