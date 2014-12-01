/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.xdr.IuCsXdr

class IuCsXdrCsvReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedIuCsXdr: RDD[ParsedItem[IuCsXdr]] = SparkParser.fromCsv[IuCsXdr](self)(IuCsXdr.fromCsv)

  def toIuCsXdr: RDD[IuCsXdr] = toParsedIuCsXdr.values

  def toIuCsXdrErrors: RDD[ParsingError] = toParsedIuCsXdr.errors
}

class IuCsXdrRowReader(self: RDD[Row]) {

  def toIuCsXdr: RDD[IuCsXdr] = SparkParser.fromRow[IuCsXdr](self)
}

class IuCsXdrWriter(self: RDD[IuCsXdr]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[IuCsXdr](self, path)
}

trait IuCsXdrDsl {

  implicit def iuCsXdrCsvReader(self: RDD[String]): IuCsXdrCsvReader = new IuCsXdrCsvReader(self)

  implicit def iuCsXdrRowReader(self: RDD[Row]): IuCsXdrRowReader = new IuCsXdrRowReader(self)

  implicit def iuCsXdrWriter(iuCsXdrs: RDD[IuCsXdr]): IuCsXdrWriter = new IuCsXdrWriter(iuCsXdrs)
}

object IuCsXdrDsl extends IuCsXdrDsl with ParsedItemsDsl
