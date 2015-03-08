/*
 * TODO: License goes here!
 */

package sa.com.mobily.utils.spark

import scala.annotation.tailrec
import scala.language.implicitConversions
import scala.util.Try

import com.github.nscala_time.time.Imports._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

import sa.com.mobily.utils.EdmCoreUtils

class RddRowFunctions(self: SQLContext) {

  @tailrec
  final def readFromPeriod(path: String, start: DateTime, end: DateTime,
      result: RDD[Row] = self.sparkContext.emptyRDD): RDD[Row] = {
    if (start == end) result.union(self.parquetFile(RddRowDsl.replaceDate(path, start)))
    else
      readFromPeriod(path, start.plusDays(1), end, result.union(self.parquetFile(RddRowDsl.replaceDate(path, start))))
  }

  def readFromPeriod(path: String, start: String, end: String): RDD[Row] =
    readFromPeriod(path, RddRowDsl.getDateFromString(start).get, RddRowDsl.getDateFromString(end).get)
}

trait RddRowDsl {

  implicit def rddRowFunctions(self: SQLContext): RddRowFunctions = new RddRowFunctions(self)
}

object RddRowDsl extends RddRowDsl {

  val DateRegex = "\\d{4}\\/\\d{2}\\/\\d{2}".r

  def replaceDate(path: String, dateTime: DateTime): String =
    DateRegex.replaceFirstIn(path, EdmCoreUtils.dateAsString(dateTime.getMillis))

  def getDateFromString(dateAsString: String): Try[DateTime] = for {
    dateString <- Try(DateRegex.findFirstIn(dateAsString))
    dateTime <- EdmCoreUtils.tryToParseTheDate(dateString.get)
  } yield dateTime

  def getDateFromPath(path: String): Try[DateTime] = for {
    dateString <- Try(DateRegex.findFirstIn(path))
    dateTime <- getDateFromString(dateString.get)
  } yield dateTime
}
