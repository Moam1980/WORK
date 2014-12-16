/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import sa.com.mobily.event.Event
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}
import sa.com.mobily.utils.EdmCoreUtils._
import sa.com.mobily.xdr._

class UfdrPsXdrCsvReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedUfdrPsXdr: RDD[ParsedItem[UfdrPsXdr]] = SparkParser.fromCsv[UfdrPsXdr](self)(UfdrPsXdr.fromCsv)

  def toUfdrPsXdr: RDD[UfdrPsXdr] = toParsedUfdrPsXdr.values

  def toUfdrPsXdrErrors: RDD[ParsingError] = toParsedUfdrPsXdr.errors

  def saveErrors(path: String): Unit = toUfdrPsXdrErrors.map(_.line).saveAsTextFile(path)
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

class UfdrPsXdrParser(self: RDD[UfdrPsXdr]) {

  def toEvent: RDD[Event] = self.filter { ufdrPs =>
    (!ufdrPs.user.imei.isEmpty ||
      !ufdrPs.user.imsi.isEmpty ||
      ufdrPs.user.msisdn > 0L) &&
      ufdrPs.duration.beginTime > 0L &&
      ufdrPs.duration.endTime > 0L &&
      (ufdrPs.cell.id._1 != UfdrPSXdrCell.NonDefined && ufdrPs.cell.id._2 != UfdrPSXdrCell.NonDefined) &&
      ufdrPs.protocol.category.identifier > 0
  }.map(_.toEvent)
}

trait UfdrPsXdrDsl {

  implicit def ufdrPsXdrCsvReader(self: RDD[String]): UfdrPsXdrCsvReader = new UfdrPsXdrCsvReader(self)

  implicit def ufdrPsXdrRowReader(self: RDD[Row]): UfdrPsXdrRowReader = new UfdrPsXdrRowReader(self)

  implicit def ufdrPsXdrWriter(ufdrPsXdrs: RDD[UfdrPsXdr]): UfdrPsXdrWriter = new UfdrPsXdrWriter(ufdrPsXdrs)

  implicit def ufdrPsXdrFunctions(ufdrPsXdrs: RDD[UfdrPsXdr]): UfdrPsXdrFunctions = new UfdrPsXdrFunctions(ufdrPsXdrs)

  implicit def ufdrPsXdrParser(ufdrPsXdrs: RDD[UfdrPsXdr]): UfdrPsXdrParser = new UfdrPsXdrParser(ufdrPsXdrs)
}

object UfdrPsXdrDsl extends UfdrPsXdrDsl with ParsedItemsDsl
