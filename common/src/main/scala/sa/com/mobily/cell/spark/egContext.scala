/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.cell.EgCell
import sa.com.mobily.parsing.{ParsingError, ParsedItem}
import sa.com.mobily.parsing.spark.{SparkCsvParser, ParsedItemsContext}

class EgCellReader(self: RDD[String]) {

  import ParsedItemsContext._

  def toParsedEgCell: RDD[ParsedItem[EgCell]] = SparkCsvParser.fromCsv[EgCell](self)

  def toEgCell: RDD[EgCell] = toParsedEgCell.values

  def toEgCellErrors: RDD[ParsingError] = toParsedEgCell.errors
}

trait EgCellContext {

  implicit def egCellReader(csv: RDD[String]): EgCellReader = new EgCellReader(csv)
}

object EgContext extends EgCellContext with ParsedItemsContext
