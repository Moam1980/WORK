/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import sa.com.mobily.xdr._
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.utils.EdmCoreUtils._

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

class UfdrPsXdrFunctions(self: RDD[UfdrPsXdr]) {

  def perHourCellUserAndProtocol: RDD[UfdrPsXdrHierarchyAgg] = {

    val transferStatsByHierarchy = self.map(event =>
      (UfdrPsXdrHierarchy(
        hourTime = parseTimestampToSaudiDate(roundTimestampHourly(event.duration.beginTime)),
        cell = event.cell,
        user = event.user,
        protocol = event.protocol),
      event.transferStats))

    transferStatsByHierarchy.reduceByKey(TransferStats.aggregate).map(event =>
      UfdrPsXdrHierarchyAgg(hierarchy = event._1, transferStats = event._2))
  }
}

trait UfdrPsXdrDsl {

  implicit def ufdrPsXdrCsvReader(self: RDD[String]): UfdrPsXdrCsvReader = new UfdrPsXdrCsvReader(self)

  implicit def ufdrPsXdrRowReader(self: RDD[Row]): UfdrPsXdrRowReader = new UfdrPsXdrRowReader(self)

  implicit def ufdrPsXdrWriter(ufdrPsXdrs: RDD[UfdrPsXdr]): UfdrPsXdrWriter = new UfdrPsXdrWriter(ufdrPsXdrs)

  implicit def ufdrPsXdrFunctions(ufdrPsXdrs: RDD[UfdrPsXdr]): UfdrPsXdrFunctions = new UfdrPsXdrFunctions(ufdrPsXdrs)
}

object UfdrPsXdrDsl extends UfdrPsXdrDsl with ParsedItemsDsl
