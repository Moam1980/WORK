/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import sa.com.mobily.parsing.{OpenCsvParser, CsvParser}
import sa.com.mobily.utils.EdmCoreUtils

case class ImsNeighboringCell(
    vendor: String,
    technology: Technology,
    cellName: String,
    neighboringCellName: String,
    qrxLevMin: Option[Int]) {

  def fields: Array[String] =
    Array(vendor, technology.identifier, cellName, neighboringCellName, qrxLevMin.getOrElse("").toString)
}

object ImsNeighboringCell {

  def header: Array[String] = Array("vendor", "technology", "cellName", "neighboringCellName", "qrxLevMin")

  final val lineCsvParserObject = new OpenCsvParser(quote = '"')

  implicit val fromCsv = new CsvParser[ImsNeighboringCell] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): ImsNeighboringCell = {
      val Array(vendorText, technologyText, cellNameText, neighboringCellNameText, qrxLevMinText) = fields

      ImsNeighboringCell(
        vendor = vendorText,
        technology = Cell.parseTechnology(technologyText),
        cellName = cellNameText,
        neighboringCellName = neighboringCellNameText,
        qrxLevMin = EdmCoreUtils.parseInt(qrxLevMinText))
    }
  }
}
