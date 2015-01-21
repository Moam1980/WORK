/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.cell.ImsNeighboringCell
import sa.com.mobily.parsing.{ParsingError, ParsedItem}
import sa.com.mobily.parsing.spark.{SparkParser, ParsedItemsDsl}

class ImsNeighboringCellReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedImsNeighboringCell: RDD[ParsedItem[ImsNeighboringCell]] = SparkParser.fromCsv[ImsNeighboringCell](self)

  def toImsNeighboringCell: RDD[ImsNeighboringCell] = toParsedImsNeighboringCell.values

  def toImsNeighboringCellErrors: RDD[ParsingError] = toParsedImsNeighboringCell.errors
}

trait ImsNeighboringCellDsl {

  implicit def imsNeighboringCellReader(csv: RDD[String]): ImsNeighboringCellReader = new ImsNeighboringCellReader(csv)
}

object ImsNeighboringCellDsl extends ImsNeighboringCellDsl with ParsedItemsDsl
