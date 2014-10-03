/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.cell.Cell
import sa.com.mobily.parsing.spark.{ParsedItemsContext, SparkCsvParser}
import sa.com.mobily.parsing.{ParsedItem, ParsingError}

class CellReader(self: RDD[String]) {

  import ParsedItemsContext._

  def toParsedCell: RDD[ParsedItem[Cell]] = SparkCsvParser.fromCsv[Cell](self)

  def toCell: RDD[Cell] = toParsedCell.values

  def toCellErrors: RDD[ParsingError] = toParsedCell.errors
}

trait CellContext {

  implicit def cellReader(csv: RDD[String]): CellReader = new CellReader(csv)
}

object CellContext extends CellContext with ParsedItemsContext
