/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.cell.SqmCell
import sa.com.mobily.parsing.{ParsingError, ParsedItem}
import sa.com.mobily.parsing.spark.{SparkParser, ParsedItemsDsl}

class SqmCellReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedSqmCell: RDD[ParsedItem[SqmCell]] = SparkParser.fromCsv[SqmCell](self)

  def toSqmCell: RDD[SqmCell] = toParsedSqmCell.values

  def toSqmCellErrors: RDD[ParsingError] = toParsedSqmCell.errors
}

trait SqmCellDsl {

  implicit def sqmCellReader(csv: RDD[String]): SqmCellReader = new SqmCellReader(csv)
}

object SqmDsl extends SqmCellDsl with ParsedItemsDsl
