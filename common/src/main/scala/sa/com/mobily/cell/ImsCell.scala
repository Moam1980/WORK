/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import sa.com.mobily.parsing.{OpenCsvParser, CsvParser}
import sa.com.mobily.utils.EdmCoreUtils

case class ImsCell(
    vendor: String,
    technology: Technology,
    nodeB: String,
    cellName: String,
    cellId: String,
    bcchFrequency: Option[Int],
    lac: Option[Int],
    rac: Option[Int],
    sac: Option[Int],
    maxTxPower: Option[Int],
    extra: String) {

  def fields: Array[String] =
    Array(vendor, technology.identifier, nodeB, cellName, cellId, bcchFrequency.getOrElse("").toString,
      lac.getOrElse("").toString, rac.getOrElse("").toString, sac.getOrElse("").toString,
      maxTxPower.getOrElse("").toString, extra)
}

object ImsCell {

  def header: Array[String] =
    Array("vendor", "technology", "nodeB", "cellName", "cellId", "bcchFrequency", "lac", "rac", "sac", "maxTxPower",
      "extra")

  final val lineCsvParserObject = new OpenCsvParser(quote = '"')

  implicit val fromCsv = new CsvParser[ImsCell] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): ImsCell = {
      val Array(vendorText, technologyText, cellNameText, cellIdText, bcchFrequencyText, lacText, racText, sacText,
        maxTxPowerText, extraText) = fields

      ImsCell(
        vendor = vendorText,
        technology = Cell.parseTechnology(technologyText),
        nodeB = parseNodeBFromCellName(cellNameText),
        cellName = cellNameText,
        cellId = cellIdText,
        bcchFrequency = EdmCoreUtils.parseInt(EdmCoreUtils.parseNullString(bcchFrequencyText)),
        lac = EdmCoreUtils.parseInt(EdmCoreUtils.parseNullString(lacText)),
        rac = EdmCoreUtils.parseInt(EdmCoreUtils.parseNullString(racText)),
        sac = EdmCoreUtils.parseInt(EdmCoreUtils.parseNullString(sacText)),
        maxTxPower = EdmCoreUtils.parseInt(EdmCoreUtils.parseNullString(maxTxPowerText)),
        extra = EdmCoreUtils.parseNullString(extraText))
    }
  }

  def parseNodeBFromCellName(cellNameText: String): String =
    if (cellNameText.contains("_"))
      if ((cellNameText.contains("VIP")) && (cellNameText.lastIndexOf("_") == 3))
        cellNameText.substring(0, cellNameText.length - 1)
      else cellNameText.substring(0, cellNameText.lastIndexOf("_"))
    else if (cellNameText.contains("-")) cellNameText.substring(0, cellNameText.lastIndexOf("-"))
      else cellNameText.substring(0, cellNameText.length - 1)
}
