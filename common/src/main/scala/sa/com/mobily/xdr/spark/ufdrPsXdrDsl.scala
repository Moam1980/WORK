/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import sa.com.mobily.xdr._
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}
import sa.com.mobily.parsing.{ParsedItem, ParsingError}

class UfdrPsXdrCsvReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedUfdrPsXdr: RDD[ParsedItem[UfdrPsXdr]] = SparkParser.fromCsv[UfdrPsXdr](self)(UfdrPsXdr.fromCsv)

  def toUfdrPsXdr: RDD[UfdrPsXdr] = toParsedUfdrPsXdr.values

  def toUfdrPsXdrErrors: RDD[ParsingError] = toParsedUfdrPsXdr.errors
}

class UfdrPsXdrRowReader(self: RDD[Row]) {

  def toUfdrPsXdr: RDD[UfdrPsXdr] = SparkParser.fromRow[UfdrPsXdr](self)
}

class UfdrPsXdrWriter(self: RDD[UfdrPsXdr]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[UfdrPsXdr](self, path)
}

trait UfdrPsXdrDsl {

  implicit def ufdrPsXdrCsvReader(self: RDD[String]): UfdrPsXdrCsvReader = new UfdrPsXdrCsvReader(self)

  implicit def ufdrPsXdrRowReader(self: RDD[Row]): UfdrPsXdrRowReader = new UfdrPsXdrRowReader(self)

  implicit def ufdrPsXdrWriter(ufdrPsXdrs: RDD[UfdrPsXdr]): UfdrPsXdrWriter = new UfdrPsXdrWriter(ufdrPsXdrs)
}

object UfdrPsXdrDsl extends UfdrPsXdrDsl with ParsedItemsDsl
