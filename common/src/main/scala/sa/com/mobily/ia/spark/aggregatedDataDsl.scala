/*
 * TODO: License goes here!
 */

package sa.com.mobily.ia.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.ia.AggregatedData
import sa.com.mobily.parsing.{ParsingError, ParsedItem}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser}

class AggregatedDataReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedAggregatedData: RDD[ParsedItem[AggregatedData]] = SparkParser.fromCsv[AggregatedData](self)

  def toAggregatedData: RDD[AggregatedData] = toParsedAggregatedData.values

  def toAggregatedDataErrors: RDD[ParsingError] = toParsedAggregatedData.errors
}

trait AggregatedDataDsl {

  implicit def aggregatedDataReader(csv: RDD[String]): AggregatedDataReader = new AggregatedDataReader(csv)
}

object AggregatedDataDsl extends AggregatedDataDsl with ParsedItemsDsl
