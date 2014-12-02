/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.xdr._

class AiCsXdrCsvReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedAiCsXdr: RDD[ParsedItem[AiCsXdr]] = SparkParser.fromCsv[AiCsXdr](self)(AiCsXdr.fromCsv)

  def toAiCsXdr: RDD[AiCsXdr] = toParsedAiCsXdr.values

  def toAiCsXdrErrors: RDD[ParsingError] = toParsedAiCsXdr.errors
}

class AiCsXdrRowReader(self: RDD[Row]) {

  def toAiCsXdr: RDD[AiCsXdr] = SparkParser.fromRow[AiCsXdr](self)
}

class AiCsXdrWriter(self: RDD[AiCsXdr]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[AiCsXdr](self, path)
}

trait AiCsXdrDsl {

  implicit def aiCsXdrCsvReader(self: RDD[String]): AiCsXdrCsvReader = new AiCsXdrCsvReader(self)

  implicit def aiCsXdrRowReader(self: RDD[Row]): AiCsXdrRowReader = new AiCsXdrRowReader(self)

  implicit def aiCsXdrWriter(aiCsXdrs: RDD[AiCsXdr]): AiCsXdrWriter = new AiCsXdrWriter(aiCsXdrs)
}

object AiCsXdrDsl extends AiCsXdrDsl with ParsedItemsDsl
