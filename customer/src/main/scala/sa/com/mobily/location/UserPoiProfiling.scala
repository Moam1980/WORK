/*
 * TODO: License goes here!
 */

package sa.com.mobily.location

import org.apache.spark.sql.Row

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser, RowParser}
import sa.com.mobily.poi.{PoiType, ProfilingPoiType}

case class UserPoiProfiling(
    imsi: String,
    poiProfiling: ProfilingPoiType) {

  def fields: Array[String] = Array(imsi, poiProfiling.identifier)
}

object UserPoiProfiling {

  val Header: Array[String] = Array("imsi", "poiProfiling")

  def apply(
      imsi: String,
      poiTypes: List[PoiType]): UserPoiProfiling =
    UserPoiProfiling(
      imsi = imsi,
      poiProfiling = ProfilingPoiType.parseUserPoiType(poiTypes))

  implicit val fromCsv = new CsvParser[UserPoiProfiling] {

    override def lineCsvParser: OpenCsvParser = new OpenCsvParser

    override def fromFields(fields: Array[String]): UserPoiProfiling = {
      val Array(imsi, poiProfiling) = fields

      UserPoiProfiling(
        imsi = imsi,
        poiProfiling = ProfilingPoiType(poiProfiling))
    }
  }

  implicit val fromRow = new RowParser[UserPoiProfiling] {

    override def fromRow(row: Row): UserPoiProfiling = {
      val Row(imsi, poiProfilingRow) = row
      val Row(poiProfiling) = poiProfilingRow.asInstanceOf[Row]

      UserPoiProfiling(
        imsi = imsi.asInstanceOf[String],
        poiProfiling = ProfilingPoiType(poiProfiling.asInstanceOf[String]))
    }
  }
}
