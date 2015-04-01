/*
 * TODO: License goes here!
 */

package sa.com.mobily.location.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import sa.com.mobily.location.UserPoiProfiling
import sa.com.mobily.parsing.{ParsedItem, ParsingError}
import sa.com.mobily.parsing.spark.{ParsedItemsDsl, SparkParser, SparkWriter}

class UserPoiProfilingCsvReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedUserPoiProfiling: RDD[ParsedItem[UserPoiProfiling]] = SparkParser.fromCsv[UserPoiProfiling](self)

  def toUserPoiProfiling: RDD[UserPoiProfiling] = toParsedUserPoiProfiling.values

  def toUserPoiProfilingErrors: RDD[ParsingError] = toParsedUserPoiProfiling.errors
}

class UserPoiProfilingWriter(self: RDD[UserPoiProfiling]) {

  def saveAsParquetFile(path: String): Unit = SparkWriter.saveAsParquetFile[UserPoiProfiling](self, path)
}

class UserPoiProfilingRowReader(self: RDD[Row]) {

  def toUserPoiProfiling: RDD[UserPoiProfiling] = SparkParser.fromRow[UserPoiProfiling](self)
}

trait UserPoiProfilingDsl {

  implicit def userPoiProfilingReader(csv: RDD[String]): UserPoiProfilingCsvReader =
    new UserPoiProfilingCsvReader(csv)

  implicit def userPoiProfilingWriter(self: RDD[UserPoiProfiling]): UserPoiProfilingWriter =
    new UserPoiProfilingWriter(self)

  implicit def userPoiProfilingRowReader(self: RDD[Row]): UserPoiProfilingRowReader =
    new UserPoiProfilingRowReader(self)
}

object UserPoiProfilingDsl extends UserPoiProfilingDsl with ParsedItemsDsl
