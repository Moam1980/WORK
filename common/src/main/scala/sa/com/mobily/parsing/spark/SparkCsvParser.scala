/*
 * TODO: License goes here!
 */

package sa.com.mobily.parsing.spark

import org.apache.spark.rdd.RDD

import sa.com.mobily.parsing.{ParsedItem, CsvParser}

object SparkCsvParser {

  def fromCsv[T](lines: RDD[String])(implicit parser: CsvParser[T]): RDD[ParsedItem[T]] =
    lines.map(line => CsvParser.fromLine(line))
}
