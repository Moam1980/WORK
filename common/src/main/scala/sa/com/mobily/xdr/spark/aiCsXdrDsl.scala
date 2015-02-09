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
import sa.com.mobily.xdr.AiCsXdr

class AiCsXdrCsvReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedAiCsXdr: RDD[ParsedItem[AiCsXdr]] = SparkParser.fromCsv[AiCsXdr](self)(AiCsXdr.fromCsv)

  def toAiCsXdr: RDD[AiCsXdr] = toParsedAiCsXdr.values

  def toAiCsXdrErrors: RDD[ParsingError] = toParsedAiCsXdr.errors

  def saveErrors(path: String): Unit = toAiCsXdrErrors.map(_.line).saveAsTextFile(path)
}

class AiCsXdrRowReader(self: RDD[Row]) {

  def toAiCsXdr: RDD[AiCsXdr] = SparkParser.fromRow[AiCsXdr](self)
}

class AiCsXdrWriter(self: RDD[AiCsXdr]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[AiCsXdr](self, path)
}

class AiCsXdrParser(self: RDD[AiCsXdr]) {

  def toEvent: RDD[Event] = self.filter { aiCs =>
    (aiCs.user.imei.isDefined ||
      aiCs.user.imsi.isDefined ||
      aiCs.user.msisdn.isDefined) &&
    !aiCs.time.csTime.begin.isEmpty &&
    !aiCs.time.csTime.end.isEmpty &&
    aiCs.cell.id._1.isDefined &&
    aiCs.cell.id._2.isDefined
  }.map(_.toEvent)

  def sanity: RDD[(String, Int)] = self.flatMap(aiCs => {
    val nonEmptyOptionString = List(
      ("imei", aiCs.user.imei),
      ("imsi", aiCs.user.imsi),
      ("cellId", aiCs.cell.firstCellId),
      ("firstLac", aiCs.cell.csCell.firstLac))
    val nonEmptyOptionLong = List(("msisdn", aiCs.user.msisdn))
    val nonEmptyString = List(("beginTime", aiCs.time.csTime.begin), ("endTime", aiCs.time.csTime.end),
      ("type", aiCs.call.scenario))

    List(("total", 1)) ++
      SanityUtils.sanityMethod[Option[String]](nonEmptyOptionString, value => !value.isDefined) ++
      SanityUtils.sanityMethod[Option[Long]](nonEmptyOptionLong, value => !value.isDefined) ++
      SanityUtils.sanityMethod[String](nonEmptyString, value => value.isEmpty)
  }).reduceByKey(_ + _)
}

trait AiCsXdrDsl {

  implicit def aiCsXdrCsvReader(self: RDD[String]): AiCsXdrCsvReader = new AiCsXdrCsvReader(self)

  implicit def aiCsXdrRowReader(self: RDD[Row]): AiCsXdrRowReader = new AiCsXdrRowReader(self)

  implicit def aiCsXdrWriter(aiCsXdrs: RDD[AiCsXdr]): AiCsXdrWriter = new AiCsXdrWriter(aiCsXdrs)

  implicit def aiCsXdrParser(aiCsXdrs: RDD[AiCsXdr]): AiCsXdrParser = new AiCsXdrParser(aiCsXdrs)
}

object AiCsXdrDsl extends AiCsXdrDsl with ParsedItemsDsl
