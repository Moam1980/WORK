/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.cell.SqmCell
import sa.com.mobily.parsing.{ParsingError, ParsedItem}
import sa.com.mobily.parsing.spark.{SparkCsvParser, ParsedItemsContext}

class SqmCellReader(self: RDD[String]) {

  import ParsedItemsContext._

  def toParsedSqmCell: RDD[ParsedItem[SqmCell]] = SparkCsvParser.fromCsv[SqmCell](self)

  def toSqmCell: RDD[SqmCell] = toParsedSqmCell.values

  def toSqmCellErrors: RDD[ParsingError] = toParsedSqmCell.errors
}

trait SqmCellContext {

  implicit def sqmCellReader(csv: RDD[String]): SqmCellReader = new SqmCellReader(csv)
}

object SqmContext extends SqmCellContext with ParsedItemsContext
