/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import sa.com.mobily.ia.AggregatedData
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}

class AggregatedDataReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedAggregatedData: RDD[ParsedItem[AggregatedData]] = SparkParser.fromCsv[AggregatedData](self)

  def toAggregatedData: RDD[AggregatedData] = toParsedAggregatedData.values

  def toAggregatedDataErrors: RDD[ParsingError] = toParsedAggregatedData.errors
}

class AggregatedDataRowReader(self: RDD[Row]) {

  def toAggregatedData: RDD[AggregatedData] = SparkParser.fromRow[AggregatedData](self)
}

class AggregatedDataWriter(self: RDD[AggregatedData]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[AggregatedData](self, path)
}

trait AggregatedDataDsl {

  implicit def aggregatedDataReader(csv: RDD[String]): AggregatedDataReader = new AggregatedDataReader(csv)

  implicit def aggregatedDataRowReader(self: RDD[Row]): AggregatedDataRowReader = new AggregatedDataRowReader(self)

  implicit def aggregatedDataWriter(aggregatedData: RDD[AggregatedData]): AggregatedDataWriter =
    new AggregatedDataWriter(aggregatedData)
}

object AggregatedDataDsl extends AggregatedDataDsl with ParsedItemsDsl
