/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.cell.ImsCell
import sa.com.mobily.parsing.{ParsingError, ParsedItem}
import sa.com.mobily.parsing.spark.{SparkParser, ParsedItemsDsl}

class ImsCellReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedImsCell: RDD[ParsedItem[ImsCell]] = SparkParser.fromCsv[ImsCell](self)

  def toImsCell: RDD[ImsCell] = toParsedImsCell.values

  def toImsCellErrors: RDD[ParsingError] = toParsedImsCell.errors
}

trait ImsCellDsl {

  implicit def imsCellReader(csv: RDD[String]): ImsCellReader = new ImsCellReader(csv)
}

object ImsCellDsl extends ImsCellDsl with ParsedItemsDsl
