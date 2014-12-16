/*
 * TODO: License goes here!
 */

package sa.com.mobily.xdr.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import sa.com.mobily.event.Event
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}
import sa.com.mobily.xdr._

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

  def toEvent: RDD[Event] = self.filter {
    aiCs => (
      aiCs.csUser.msisdn.isDefined &&
        !aiCs.aiTime.csTime.begin.isEmpty &&
        !aiCs.aiTime.csTime.end.isEmpty &&
        aiCs.aiCell.csCell.firstLac.isDefined &&
        aiCs.aiCell.firstCellId.isDefined &&
        aiCs.aiCall.csCall.callType.isDefined)
  }.map { _.toEvent }
}

trait AiCsXdrDsl {

  implicit def aiCsXdrCsvReader(self: RDD[String]): AiCsXdrCsvReader = new AiCsXdrCsvReader(self)

  implicit def aiCsXdrRowReader(self: RDD[Row]): AiCsXdrRowReader = new AiCsXdrRowReader(self)

  implicit def aiCsXdrWriter(aiCsXdrs: RDD[AiCsXdr]): AiCsXdrWriter = new AiCsXdrWriter(aiCsXdrs)

  implicit def aiCsXdrParser(aiCsXdrs: RDD[AiCsXdr]): AiCsXdrParser = new AiCsXdrParser(aiCsXdrs)
}

object AiCsXdrDsl extends AiCsXdrDsl with ParsedItemsDsl
