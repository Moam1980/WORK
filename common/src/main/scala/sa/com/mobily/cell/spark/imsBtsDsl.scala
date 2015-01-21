/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import sa.com.mobily.cell.ImsBts
import sa.com.mobily.parsing.{ParsingError, ParsedItem}
import sa.com.mobily.parsing.spark.{SparkParser, ParsedItemsDsl}

class ImsBtsReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedImsBts: RDD[ParsedItem[ImsBts]] = SparkParser.fromCsv[ImsBts](self)

  def toImsBts: RDD[ImsBts] = toParsedImsBts.values

  def toImsBtsErrors: RDD[ParsingError] = toParsedImsBts.errors
}

class ImsBtsFunctions(self: RDD[ImsBts]) {

  def mergeImsBts: RDD[ImsBts] =
    self.map(imsBts => ((imsBts.vendor, imsBts.technology, imsBts.id), imsBts)).reduceByKey(ImsBts.merge).map(_._2)
}

trait ImsBtsDsl {

  implicit def imsBtsReader(csv: RDD[String]): ImsBtsReader = new ImsBtsReader(csv)

  implicit def imsBtsFunctions(imsBts: RDD[ImsBts]): ImsBtsFunctions = new ImsBtsFunctions(imsBts)
}

object ImsBtsDsl extends ImsBtsDsl with ParsedItemsDsl
