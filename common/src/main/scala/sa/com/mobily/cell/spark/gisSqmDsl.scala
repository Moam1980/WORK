/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell.spark

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import sa.com.mobily.cell.{GisSqmSite, GisSqmCell}
import sa.com.mobily.parsing.{ParsingError, ParsedItem}
import sa.com.mobily.parsing.spark.{SparkCsvParser, ParsedItemsDsl}

class GisSqmCellReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedGisSqmCell: RDD[ParsedItem[GisSqmCell]] = SparkCsvParser.fromCsv[GisSqmCell](self)

  def toGisSqmCell: RDD[GisSqmCell] = toParsedGisSqmCell.values

  def toGisSqmCellErrors: RDD[ParsingError] = toParsedGisSqmCell.errors
}

class GisSqmSiteReader(self: RDD[String]) {

  import ParsedItemsDsl._

  def toParsedGisSqmSite: RDD[ParsedItem[GisSqmSite]] = SparkCsvParser.fromCsv[GisSqmSite](self)

  def toGisSqmSite: RDD[GisSqmSite] = toParsedGisSqmSite.values

  def toGisSqmSiteErrors: RDD[ParsingError] = toParsedGisSqmSite.errors
}

trait GisSqmCellDsl {

  implicit def gisSqmCellReader(csv: RDD[String]): GisSqmCellReader = new GisSqmCellReader(csv)
}

trait GisSqmSiteDsl {

  implicit def gisSqmSiteReader(csv: RDD[String]): GisSqmSiteReader = new GisSqmSiteReader(csv)
}

object GisSqmDsl extends GisSqmCellDsl with GisSqmSiteDsl with ParsedItemsDsl
