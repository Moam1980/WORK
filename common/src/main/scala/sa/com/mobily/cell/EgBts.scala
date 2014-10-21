/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import sa.com.mobily.geometry.{LatLongCoordinates, UtmCoordinates}
import sa.com.mobily.parsing.{OpenCsvParser, CsvParser}
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
    outdoorCov: Double)

object EgBts {

  final val lineCsvParserObject = new OpenCsvParser(separator = '\t', quote = '"')

  implicit val fromCsv = new CsvParser[EgBts] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): EgBts = {
      val (firstChunk, remaining) = fields.splitAt(19) // scalastyle:ignore magic.number
      val (secondChunk, thirdChunk) = remaining.splitAt(17) // scalastyle:ignore magic.number
      val Array(_, _, bts, code, status, longitude, latitude, onAirDate, bscId, lac, btsType, vendor, _, _,
        _, rnc, _, _, _) = firstChunk
      val Array(_, _, _, _, _, cgi, _, _, _, _, _, _, _, technology, height, _, cellType) = secondChunk
      val Array(_, _, _, _, _, _, _, _, _, _, _, indoorCov, outdoorCov, _) = thirdChunk

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
