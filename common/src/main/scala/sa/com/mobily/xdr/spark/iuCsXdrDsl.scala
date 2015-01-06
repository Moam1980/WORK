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
import sa.com.mobily.utils.SanityUtils
import sa.com.mobily.xdr.IuCsXdr

class IuCsXdrCsvReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedIuCsXdr: RDD[ParsedItem[IuCsXdr]] = SparkParser.fromCsv[IuCsXdr](self)(IuCsXdr.fromCsv)

  def toIuCsXdr: RDD[IuCsXdr] = toParsedIuCsXdr.values

  def toIuCsXdrErrors: RDD[ParsingError] = toParsedIuCsXdr.errors

  def saveErrors(path: String): Unit = toIuCsXdrErrors.map(_.line).saveAsTextFile(path)
}

class IuCsXdrRowReader(self: RDD[Row]) {

  def toIuCsXdr: RDD[IuCsXdr] = SparkParser.fromRow[IuCsXdr](self)
}

class IuCsXdrWriter(self: RDD[IuCsXdr]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[IuCsXdr](self, path)
}

class IuCsXdrParser(self: RDD[IuCsXdr]) {

  def toEvent: RDD[Event] = self.filter { iuCs =>
    (iuCs.user.imei.isDefined ||
      iuCs.user.imsi.isDefined ||
      iuCs.user.msisdn.isDefined) &&
    !iuCs.time.csTime.begin.isEmpty &&
    !iuCs.time.csTime.end.isEmpty &&
    iuCs.cell.id._1.isDefined &&
    iuCs.cell.id._2.isDefined
  }.map(_.toEvent)

  def sanity: RDD[(String, Int)] = self.flatMap(iuCs => {
    val nonEmptyOptionString = List(
      ("imei", iuCs.user.imei),
      ("imsi", iuCs.user.imsi),
      ("firstLac", iuCs.cell.csCell.firstLac),
      ("firstSac", iuCs.cell.firstSac))
      val nonEmptyOptionLong = List(("msisdn", iuCs.user.msisdn))
      val nonEmptyShort = List(("type", iuCs.ttype.iuDialogue))
      val nonEmptyString = List(
        ("beginTime", iuCs.time.csTime.begin),
        ("endTime", iuCs.time.csTime.end),
        ("dialogueIndicator", iuCs.connection.dialogueIndicator))

    List(("total", 1)) ++
      SanityUtils.sanityMethod[Option[String]](nonEmptyOptionString, value => value.isEmpty) ++
      SanityUtils.sanityMethod[Option[Long]](nonEmptyOptionLong, value => !value.isDefined) ++
      SanityUtils.sanityMethod[Short](nonEmptyShort, value => !value.isInstanceOf[Short]) ++
      SanityUtils.sanityMethod[String](nonEmptyString, value => value.isEmpty)
  }).reduceByKey(_ + _)
}

trait IuCsXdrDsl {

  implicit def iuCsXdrCsvReader(self: RDD[String]): IuCsXdrCsvReader = new IuCsXdrCsvReader(self)

  implicit def iuCsXdrRowReader(self: RDD[Row]): IuCsXdrRowReader = new IuCsXdrRowReader(self)

  implicit def iuCsXdrWriter(iuCsXdrs: RDD[IuCsXdr]): IuCsXdrWriter = new IuCsXdrWriter(iuCsXdrs)

  implicit def iuCsXdrParser(iuCsXdrs: RDD[IuCsXdr]): IuCsXdrParser = new IuCsXdrParser(iuCsXdrs)
}

object IuCsXdrDsl extends IuCsXdrDsl with ParsedItemsDsl
