/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import sa.com.mobily.geometry.{UtmCoordinates, LatLongCoordinates}
import sa.com.mobily.parsing.{OpenCsvParser, CsvParser}
import sa.com.mobily.utils.EdmCoreUtils

case class SqmCell(
    cellId: Int,
    ci: String,
    cellName: String,
    nodeId: String,
    nodeName: String,
    lacTac: Int,
    basePlanarCoords: UtmCoordinates,
    vendor: String,
    technology: Technology,
    cellType: CellType,
    height: Double,
    azimuth: Double,
    bscRncNme: String,
    region: String,
    antennaType: String,
    bspwrPcpichPmax: Double,
    accMin: Int,
    band: Int)

object SqmCell {

  final val lineCsvParserObject = new OpenCsvParser(separator = ',', quote = '"')

  implicit val fromCsv = new CsvParser[SqmCell]() {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): SqmCell = {
      val Array(cellNameText, cellIdText, nodeIdText, ciText, nodeNameText, lacTacText, latitudeText, longitudeText,
        vendorText, techText, cellTypeText, heightText, azimuthText, bscRncNmeText, regionText, antennaTypeText,
        bspwrPcpichPmaxText, accminRxlevminText, bandText) = fields

      val coords = LatLongCoordinates(latitudeText.toDouble, longitudeText.toDouble).utmCoordinates()

      SqmCell(
        cellIdText.toInt,
        ciText,
        cellNameText,
        nodeIdText,
        nodeNameText,
        lacTacText.toInt,
        coords,
        vendorText,
        Cell.parseTechnology(techText),
        Cell.parseCellType(cellTypeText),
        EdmCoreUtils.parseDouble(heightText).getOrElse(0.0),
        EdmCoreUtils.parseDouble(azimuthText).getOrElse(0.0),
        bscRncNmeText,
        regionText,
        antennaTypeText,
        EdmCoreUtils.parseDouble(bspwrPcpichPmaxText).getOrElse(0.0),
        EdmCoreUtils.parseInt(accminRxlevminText).getOrElse(0),
        EdmCoreUtils.parseInt(bandText).getOrElse(0))
    }
  }
}
