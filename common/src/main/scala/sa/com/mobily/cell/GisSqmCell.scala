/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import sa.com.mobily.parsing.{OpenCsvParser, CsvParser}
import sa.com.mobily.utils.EdmCoreUtils

case class GisSqmCell(
    cgi: String,
    cellId: String,
    cellName: String,
    nodeId: String,
    accMin: Int,
    antennaType: String,
    powerCell: String,
    cellDirection: String,
    sysType: String,
    technology: Technology,
    cellType: CellType,
    height: Double)

object GisSqmCell {

  final val lineCsvParserObject = new OpenCsvParser

  implicit val fromCsv = new CsvParser[GisSqmCell] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): GisSqmCell = {
      val Array(cgiText, cellIdText, cellNameText, nodeIdText, accMinText, antennaTypeText, powerCellText,
      cellDirectionText, sysTypeText, techText, cellTypeText, heightText) = fields

      GisSqmCell(cgiText.trim, cellIdText, cellNameText, nodeIdText, parseAccMin(accMinText), antennaTypeText,
        powerCellText, cellDirectionText, sysTypeText, Cell.parseTechnology(techText), Cell.parseCellType(cellTypeText),
        parseHeight(heightText))
    }
  }

  private def parseAccMin(text: String): Int = EdmCoreUtils.parseInt(text).getOrElse(0)

  private def parseHeight(text: String): Double = EdmCoreUtils.parseDouble(text).getOrElse(0.0)
}
