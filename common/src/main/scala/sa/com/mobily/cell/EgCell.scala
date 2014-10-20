/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import sa.com.mobily.parsing.{OpenCsvParser, CsvParser}
import sa.com.mobily.utils.EdmCoreUtils

case class EgCellCodeDesc(
    vendor: String,
    btsCodeType: String,
    btsCodeId: String,
    btsCodeValue: String,
    btsCodeDescription: String,
    btsStatusId: String,
    btsStatusCode: String,
    btsStatusName1: String,
    btsStatusName2: String,
    cellVendorId: String,
    cellVendorCode: String,
    cellVendorName1: String,
    cellVendorName2: String)

case class EgCell(
    btsId: String,
    cellCode: String,
    direction: Double,
    height: Double,
    lac: String,
    cellName: String,
    descInArabic: String,
    onAirDate: String,
    remarks: String,
    serviceDistance: String,
    sgsn: String,
    status: String,
    tilt: Double,
    cellType: String,
    shape: String,
    lat: Double,
    long: Double,
    codeDesc: EgCellCodeDesc)

object EgCell {

  val DefaultTilt = 0.0

  final val lineCsvParserObject = new OpenCsvParser(quote = '"')

  implicit val fromCsv = new CsvParser[EgCell] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): EgCell = {
      val (egCellInfo, codeDescInfo) = fields.splitAt(21) // scalastyle:ignore magic.number
      val Array(objectId, id, code, lac, name1, name2, typeId, statusId, vendorId, sgsn, onAirDate, direction,
        tilt, height, serviceDistance, remarks, cellType, status, vendor, btsId, shapeText) = egCellInfo
      val Array(btsCodeType, btsCodeId, btsCodeValue, btsCodeDescription, btsStatusId, btsStatusCode,
      btsStatusName1, btsStatusName2, cellVendorId, cellVendorCode, cellVendorName1, cellVendorName2) = codeDescInfo

      val (latitude, longitude) = latLongFromGeomBlob(shapeText)
      EgCell(
        btsId = btsId.trim,
        cellCode = code.trim,
        direction = direction.toDouble,
        height = height.toDouble,
        lac = lac.trim,
        cellName = name1.trim,
        descInArabic = name2.trim,
        onAirDate = onAirDate.trim,
        remarks = remarks.trim,
        serviceDistance = serviceDistance.trim,
        sgsn = sgsn.trim,
        status = status.trim,
        tilt = EdmCoreUtils.parseDouble(tilt).getOrElse(DefaultTilt),
        cellType = cellType.trim,
        shape = shapeText,
        lat = latitude,
        long = longitude,
        codeDesc = EgCellCodeDesc(
          vendor = vendor.trim,
          btsCodeType = btsCodeType.trim,
          btsCodeId = btsCodeId.trim,
          btsCodeValue = btsCodeValue.trim,
          btsCodeDescription = btsCodeDescription.trim,
          btsStatusId = btsStatusId.trim,
          btsStatusCode = btsStatusCode.trim,
          btsStatusName1 = btsStatusName1.trim,
          btsStatusName2 = btsStatusName2.trim,
          cellVendorId = cellVendorId.trim,
          cellVendorCode = cellVendorCode.trim,
          cellVendorName1 = cellVendorName1.trim,
          cellVendorName2 = cellVendorName2.trim))
    }
  }

  private def latLongFromGeomBlob(geomBlobText: String): (Double, Double) = {
    val args = geomBlobText.split(",")
    (EdmCoreUtils.parseDouble(args(3).trim).getOrElse(Double.NaN),
      EdmCoreUtils.parseDouble(args(2).trim).getOrElse(Double.NaN))
  }
}
