/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.cell.{EgBts, EgCell}
import sa.com.mobily.parsing.{ParsingError, ParsedItem}
import sa.com.mobily.parsing.spark.{SparkCsvParser, ParsedItemsDsl}

class EgCellReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedEgCell: RDD[ParsedItem[EgCell]] = SparkCsvParser.fromCsv[EgCell](self)

  def toEgCell: RDD[EgCell] = toParsedEgCell.values

  def toEgCellErrors: RDD[ParsingError] = toParsedEgCell.errors
}

trait EgCellDsl {

  implicit def egCellReader(csv: RDD[String]): EgCellReader = new EgCellReader(csv)
}

class EgBtsReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedEgBts: RDD[ParsedItem[EgBts]] = SparkCsvParser.fromCsv[EgBts](self)

  def toEgBts: RDD[EgBts] = toParsedEgBts.values

  def toEgBtsErrors: RDD[ParsingError] = toParsedEgBts.errors
}

trait EgBtsDsl {

  implicit def egBtsReader(csv: RDD[String]): EgBtsReader = new EgBtsReader(csv)
}

object EgDsl extends EgCellDsl with EgBtsDsl with ParsedItemsDsl
