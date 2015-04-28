/*
 * TODO: License goes here!
 */

package sa.com.mobily.utils

import org.apache.commons.math3.stat.StatUtils
import org.apache.spark.sql._

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}

case class Stats(
    count: Long,
    max: Double,
    min: Double,
    mean: Double,
    variance: Double,
    stDev: Double,
    percentile99: Double,
    percentile95: Double,
    percentile90: Double,
    percentile75: Double,
    percentile50: Double,
    percentile25: Double,
    percentile10: Double,
    percentile5: Double) {

  def fields: Array[String] =
    Array(count.toString,
      max.toString,
      min.toString,
      mean.toString,
      variance.toString,
      stDev.toString,
      percentile99.toString,
      percentile95.toString,
      percentile90.toString,
      percentile75.toString,
      percentile50.toString,
      percentile25.toString,
      percentile10.toString,
      percentile5.toString)
}

object Stats {

  val Header: Array[String] =
    Array("count", "max", "min", "mean", "variance", "stDev", "percentile99", "percentile95", "percentile90",
      "percentile75", "percentile50", "percentile25", "percentile10", "percentile5")

  final val lineCsvParserObject = new OpenCsvParser(quote = '"')

  def apply(array: Array[Double]): Stats = {
    if (array.isEmpty)
      Stats(
        count = 0L,
        max = 0D,
        min = 0D,
        mean = 0D,
        variance = 0D,
        stDev = 0D,
        percentile99 = 0D,
        percentile95 = 0D,
        percentile90 = 0D,
        percentile75 = 0D,
        percentile50 = 0D,
        percentile25 = 0D,
        percentile10 = 0D,
        percentile5 = 0D)
    else {
      val variance = StatUtils.populationVariance(array)

      Stats(
        count = array.length,
        max = StatUtils.max(array),
        min = StatUtils.min(array),
        mean = StatUtils.mean(array),
        variance = variance,
        stDev = Math.sqrt(variance),
        percentile99 = StatUtils.percentile(array, 99D),
        percentile95 = StatUtils.percentile(array, 95D),
        percentile90 = StatUtils.percentile(array, 90D),
        percentile75 = StatUtils.percentile(array, 75D),
        percentile50 = StatUtils.percentile(array, 50D),
        percentile25 = StatUtils.percentile(array, 25D),
        percentile10 = StatUtils.percentile(array, 10D),
        percentile5 = StatUtils.percentile(array, 5D))
    }
  }

  def aggByMean(array: Array[Stats]): Stats = apply(array.map(_.mean))

  implicit val fromCsv = new CsvParser[Stats] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): Stats = {
      val Array(countText, maxText, minText, meanText, varianceText, stDevText, percentile99Text, percentile95Text,
        percentile90Text, percentile75Text, percentile50Text, percentile25Text, percentile10Text,
        percentile5Text) = fields

      Stats(
        count = countText.toLong,
        max = maxText.toDouble,
        min = minText.toDouble,
        mean = meanText.toDouble,
        variance = varianceText.toDouble,
        stDev = stDevText.toDouble,
        percentile99 = percentile99Text.toDouble,
        percentile95 = percentile95Text.toDouble,
        percentile90 = percentile90Text.toDouble,
        percentile75 = percentile75Text.toDouble,
        percentile50 = percentile50Text.toDouble,
        percentile25 = percentile25Text.toDouble,
        percentile10 = percentile10Text.toDouble,
        percentile5 = percentile5Text.toDouble)
    }
  }

  implicit val fromRow = new RowParser[Stats] {

    override def fromRow(row: Row): Stats = {
      val Row(count, max, min, mean,variance, stDev, percentile99, percentile95, percentile90, percentile75,
        percentile50, percentile25, percentile10, percentile5) = row

      Stats(
        count = count.asInstanceOf[Long],
        max = max.asInstanceOf[Double],
        min = min.asInstanceOf[Double],
        mean = mean.asInstanceOf[Double],
        variance = variance.asInstanceOf[Double],
        stDev = stDev.asInstanceOf[Double],
        percentile99 = percentile99.asInstanceOf[Double],
        percentile95 = percentile95.asInstanceOf[Double],
        percentile90 = percentile90.asInstanceOf[Double],
        percentile75 = percentile75.asInstanceOf[Double],
        percentile50 = percentile50.asInstanceOf[Double],
        percentile25 = percentile25.asInstanceOf[Double],
        percentile10 = percentile10.asInstanceOf[Double],
        percentile5 = percentile5.asInstanceOf[Double])
    }
  }
}
