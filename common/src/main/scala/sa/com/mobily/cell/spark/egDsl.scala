/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import sa.com.mobily.cell.{EgBts, EgCell}
import sa.com.mobily.parsing.{ParsingError, ParsedItem}
import sa.com.mobily.parsing.spark.{SparkParser, ParsedItemsDsl}
import sa.com.mobily.utils.EdmCoreUtils

class EgCellReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedEgCell: RDD[ParsedItem[EgCell]] = SparkParser.fromCsv[EgCell](self)

  def toEgCell: RDD[EgCell] = toParsedEgCell.values

  def toEgCellErrors: RDD[ParsingError] = toParsedEgCell.errors
}

trait EgCellDsl {

  implicit def egCellReader(csv: RDD[String]): EgCellReader = new EgCellReader(csv)
}

class EgBtsReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedEgBts: RDD[ParsedItem[EgBts]] = SparkParser.fromCsv[EgBts](self)

  def toEgBts: RDD[EgBts] = toParsedEgBts.values

  def toEgBtsErrors: RDD[ParsingError] = toParsedEgBts.errors
}

class EgBtsFunctions(self: RDD[EgBts]) {

  def toBroadcastMapWithRegion: Broadcast[Map[(String, String), Iterable[EgBts]]] =
    self.sparkContext.broadcast(
      self.keyBy(b => (b.bts, EdmCoreUtils.regionId(b.lac))).groupByKey.collect.toMap)
}

trait EgBtsDsl {

  implicit def egBtsReader(csv: RDD[String]): EgBtsReader = new EgBtsReader(csv)

  implicit def egBtsFunctions(egBts: RDD[EgBts]): EgBtsFunctions = new EgBtsFunctions(egBts)
}

object EgDsl extends EgCellDsl with EgBtsDsl with ParsedItemsDsl
