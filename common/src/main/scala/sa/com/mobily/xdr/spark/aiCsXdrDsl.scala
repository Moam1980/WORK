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
    (aiCs.csUser.imei.isDefined ||
      aiCs.csUser.imsi.isDefined ||
      aiCs.csUser.msisdn.isDefined) &&
    !aiCs.aiTime.csTime.begin.isEmpty &&
    !aiCs.aiTime.csTime.end.isEmpty &&
    aiCs.aiCell.csCell.firstLac.isDefined &&
    aiCs.aiCell.firstCellId.isDefined &&
    aiCs.aiCall.csCall.callType.isDefined
  }.map(_.toEvent)

  def sanity: RDD[(String, Int)] = self.flatMap(aiCs => {
    val nonEmptyOptionString = List(
      ("imei", aiCs.csUser.imei),
      ("imsi", aiCs.csUser.imsi),
      ("cellId", aiCs.aiCell.firstCellId),
      ("firstLac", aiCs.aiCell.csCell.firstLac))
    val nonEmptyOptionShort = List(("type", aiCs.aiCall.csCall.callType))
    val nonEmptyOptionLong = List(("msisdn", aiCs.csUser.msisdn))
    val nonEmptyString = List(("beginTime", aiCs.aiTime.csTime.begin), ("endTime", aiCs.aiTime.csTime.end))

    List(("total", 1)) ++
      SanityUtils.sanityMethod[Option[String]](nonEmptyOptionString, value => !value.isDefined) ++
      SanityUtils.sanityMethod[Option[Short]](nonEmptyOptionShort, value => !value.isDefined) ++
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
