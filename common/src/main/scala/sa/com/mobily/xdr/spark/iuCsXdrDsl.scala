/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr.spark

import scala.language.implicitConversions

import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import sa.com.mobily.event.Event
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}
import sa.com.mobily.utils.SanityUtils
import sa.com.mobily.xdr.{CsXdr, IuCsXdr}

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

  def toEvent: RDD[Event] = self.filter(iuCs => IuCsXdr.isValidToBeParsedAsEvent(iuCs)).map(_.toEvent)

  def toEventWithMatchingSubscribers(implicit bcSubscribersCatalogue: Broadcast[Map[String, Long]]): RDD[Event] =
    toEvent.map(e => CsXdr.fillUserEventWithMsisdn(bcSubscribersCatalogue.value, e))

  def sanity: RDD[(String, Int)] = self.flatMap(iuCs => {
    val nonEmptyOptionString = List(
      ("imei", iuCs.user.imei),
      ("imsi", iuCs.user.imsi),
      ("firstLac", iuCs.cell.csCell.firstLac),
      ("firstSac", iuCs.cell.firstSac))
      val nonEmptyOptionLong = List(("msisdn", iuCs.user.msisdn))
      val nonEmptyShort = List(("type", iuCs.ttype.iuDialogue))
      val nonEmptyString = List(("dialogueIndicator", iuCs.connection.dialogueIndicator))

    List(("total", 1)) ++
      SanityUtils.perform[Option[String]](nonEmptyOptionString, value => value.isEmpty) ++
      SanityUtils.perform[Option[Long]](nonEmptyOptionLong, value => !value.isDefined) ++
      SanityUtils.perform[Short](nonEmptyShort, value => !value.isInstanceOf[Short]) ++
      SanityUtils.perform[String](nonEmptyString, value => value.isEmpty)
  }).reduceByKey(_ + _)
}

trait IuCsXdrDsl {

  implicit def iuCsXdrCsvReader(self: RDD[String]): IuCsXdrCsvReader = new IuCsXdrCsvReader(self)

  implicit def iuCsXdrRowReader(self: RDD[Row]): IuCsXdrRowReader = new IuCsXdrRowReader(self)

  implicit def iuCsXdrWriter(iuCsXdrs: RDD[IuCsXdr]): IuCsXdrWriter = new IuCsXdrWriter(iuCsXdrs)

  implicit def iuCsXdrParser(iuCsXdrs: RDD[IuCsXdr]): IuCsXdrParser = new IuCsXdrParser(iuCsXdrs)
}

object IuCsXdrDsl extends IuCsXdrDsl with ParsedItemsDsl
