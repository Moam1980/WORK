/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import sa.com.mobily.geometry.{UtmCoordinates, LatLongCoordinates}
import sa.com.mobily.parsing.CsvParser
import sa.com.mobily.utils.EdmCoreUtils

case class SqmCell(
    cellId: String,
    ci: String,
    cellName: String,
    nodeId: String,
    nodeName: String,
    lacTac: String,
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

  implicit val fromCsv = new CsvParser[SqmCell] {

    override val delimiter: String = ","

    override def fromFields(fields: Array[String]): SqmCell = {
      val Array(cellNameText, cellIdText, nodeIdText, ciText, nodeNameText, lacTacText, latitudeText, longitudeText,
      vendorText, techText, cellTypeText, heightText, azimuthText, bscRncNmeText, regionText, antennaTypeText,
      bspwrPcpichPmaxText, accminRxlevminText, bandText) = fields

      val coords = LatLongCoordinates(EdmCoreUtils.removeQuotes(latitudeText).toDouble,
        EdmCoreUtils.removeQuotes(longitudeText).toDouble).utmCoordinates()

      SqmCell(EdmCoreUtils.removeQuotes(cellIdText), EdmCoreUtils.removeQuotes(ciText),
        EdmCoreUtils.removeQuotes(cellNameText), EdmCoreUtils.removeQuotes(nodeIdText),
        EdmCoreUtils.removeQuotes(nodeNameText), EdmCoreUtils.removeQuotes(lacTacText), coords,
        EdmCoreUtils.removeQuotes(vendorText), Cell.parseTechnology(EdmCoreUtils.removeQuotes(techText)),
        Cell.parseCellType(EdmCoreUtils.removeQuotes(cellTypeText)),
        EdmCoreUtils.parseDouble(EdmCoreUtils.removeQuotes(heightText)).getOrElse(0.0),
        EdmCoreUtils.parseDouble(EdmCoreUtils.removeQuotes(azimuthText)).getOrElse(0.0),
        EdmCoreUtils.removeQuotes(bscRncNmeText), EdmCoreUtils.removeQuotes(regionText),
        EdmCoreUtils.removeQuotes(antennaTypeText),
        EdmCoreUtils.parseDouble(bspwrPcpichPmaxText).getOrElse(0.0),
        EdmCoreUtils.parseInt(accminRxlevminText).getOrElse(0),
        EdmCoreUtils.parseInt(EdmCoreUtils.removeQuotes(bandText)).getOrElse(0))
    }
  }
}
