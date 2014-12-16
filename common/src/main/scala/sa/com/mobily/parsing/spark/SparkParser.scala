/*
 * TODO: License goes here!
 */

package sa.com.mobily.parsing.spark

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import sa.com.mobily.parsing.{CsvParser, ParsedItem, RowParser}

object SparkParser {

  def fromCsv[T](lines: RDD[String])(implicit parser: CsvParser[T]): RDD[ParsedItem[T]] =
    lines.map(line => CsvParser.fromLine(line))

  def fromRow[T:ClassTag](rows: RDD[Row])(implicit parser: RowParser[T]): RDD[T] =
    rows.map(row => RowParser.fromRow(row))
}
