/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import sa.com.mobily.cell.spark.CellMerger
import sa.com.mobily.geometry.{GeomUtils, LatLongCoordinates, UtmCoordinates}
import sa.com.mobily.parsing.{CsvParser, OpenCsvParser}
import sa.com.mobily.utils.EdmCoreUtils

case class EgBts(
    bts: String,
    code: String,
    status: String,
    coords: UtmCoordinates,
    onAirDate: String,
    bscId: String,
    lac: Int,
    btsType: String,
    vendor: String,
    rnc: String,
    cgi: String,
    technology: Technology,
    height: String,
    cellType: String,
    indoorCov: Double,
    outdoorCov: Double) {

  lazy val geom =
    GeomUtils.circle(coords.geometry, outdoorCov * CellMerger.DefaultCoverageRangeIncrementFactor)
}

object EgBts {

  final val lineCsvParserObject = new OpenCsvParser(separator = ',')

  implicit val fromCsv = new CsvParser[EgBts] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): EgBts = {
      val (firstChunk, remaining) = fields.splitAt(19) // scalastyle:ignore magic.number
      val (secondChunk, thirdChunk) = remaining.splitAt(17) // scalastyle:ignore magic.number
      val Array(_, _, _, _, _, _, bscId, bts, cellType, _, cgi, code, _, _, _, _, _, _, height) = firstChunk
      val Array(_, _, indoorCov, lac, latitude, _, longitude, _, _, _, _, _, _, onAirDate, outdoorCov, _, rnc) =
        secondChunk
      val Array(_, _, _, _, status, _, technology, _, btsType, _, vendor, _) = thirdChunk

      EgBts(
        bts = bts,
        code = code,
        status = status,
        coords = LatLongCoordinates(latitude.toDouble, longitude.toDouble).utmCoordinates(),
        onAirDate = onAirDate,
        bscId = bscId,
        lac = lac.toInt,
        btsType = btsType,
        vendor = vendor,
        rnc = rnc,
        cgi = cgi,
        technology = Cell.parseTechnology(technology),
        height = height,
        cellType = cellType,
        indoorCov = EdmCoreUtils.parseDouble(indoorCov).getOrElse(0),
        outdoorCov = outdoorCov.toDouble)
    }
  }
}
