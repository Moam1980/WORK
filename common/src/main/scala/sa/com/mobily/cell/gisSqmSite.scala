/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import sa.com.mobily.geometry.{LatLongCoordinates, UtmCoordinates}
import sa.com.mobily.parsing.{OpenCsvParser, CsvParser}

case class PoolInfo(
    mscPool: String,
    ggsnPool: String,
    sgsnPool: String,
    mmePool: String,
    ugwPool: String)

case class GisSqmSite(
    siteCgi: String,
    nodeId: String,
    nodeName: String,
    district: String,
    city: String,
    emirate: String,
    governorate: String,
    lac: String,
    planarCoords: UtmCoordinates,
    vendor: String,
    technology: Technology,
    cellType: CellType,
    height: Double,
    bscRnc: String,
    region: String,
    siteType: String,
    sitePriority: String,
    poolInfo: PoolInfo,
    status: String,
    servingArea: String,
    locationType: String,
    optmCity: String)

object GisSqmSite {

  final val lineCsvParserObject = new OpenCsvParser

  implicit val fromCsv = new CsvParser[GisSqmSite] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): GisSqmSite = {
      val (firstChunk, secondChunk) = fields.splitAt(10) // scalastyle:ignore magic.number
      val Array(siteCgiText, nodeIdText, nodeNameText, districtText, cityText, emirateText, governorateText,
        lacText, latText, longText) = firstChunk
      val Array(vendorText, techText, cellTypeText, heightText, bscRncText, regionText, siteTypeText,
        sitePriorityText, mscPoolText, ggsnPoolText, sgsnPoolText, mmePoolText, ugwPoolText, statusText,
        servingAreaText, locationTypeText, optmCityText) = secondChunk
      val coords = LatLongCoordinates(latText.toDouble, longText.toDouble).utmCoordinates()

      GisSqmSite(siteCgiText.trim, nodeIdText, nodeNameText, districtText, cityText, emirateText, governorateText,
        lacText.trim, coords, vendorText, Cell.parseTechnology(techText), Cell.parseCellType(cellTypeText),
        heightText.toDouble, bscRncText, regionText, siteTypeText, sitePriorityText,
        PoolInfo(mscPoolText, ggsnPoolText, sgsnPoolText, mmePoolText, ugwPoolText), statusText, servingAreaText,
        locationTypeText, optmCityText)
    }
  }
}
