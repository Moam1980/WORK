/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import sa.com.mobily.parsing.CsvParser
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

  implicit val fromCsv = new CsvParser[EgCell] {

    override val delimiter: String = "\\|"

    override def fromFields(fields: Array[String]): EgCell = {
      val (egCellInfo, codeDescInfo) = fields.splitAt(21) // scalastyle:ignore magic.number
      val Array(objectId, id, code, lac, name1, name2, typeId, statusId, vendorId, sgsn, onAirDate, direction,
        tilt, height, serviceDistance, remarks, cellType, status, vendor, btsId, shapeText) = egCellInfo
      val Array(btsCodeType, btsCodeId, btsCodeValue, btsCodeDescription, btsStatusId, btsStatusCode,
      btsStatusName1, btsStatusName2, cellVendorId, cellVendorCode, cellVendorName1, cellVendorName2) = codeDescInfo

      val (latitude, longitude) = latLongFromGeomBlob(shapeText)
      EgCell(
        btsId = EdmCoreUtils.removeQuotes(btsId.trim),
        cellCode = EdmCoreUtils.removeQuotes(code.trim),
        direction = EdmCoreUtils.removeQuotes(direction).toDouble,
        height = EdmCoreUtils.removeQuotes(height).toDouble,
        lac = EdmCoreUtils.removeQuotes(lac.trim),
        cellName = EdmCoreUtils.removeQuotes(name1.trim),
        descInArabic = EdmCoreUtils.removeQuotes(name2.trim),
        onAirDate = EdmCoreUtils.removeQuotes(onAirDate.trim),
        remarks = EdmCoreUtils.removeQuotes(remarks.trim),
        serviceDistance = EdmCoreUtils.removeQuotes(serviceDistance.trim),
        sgsn = EdmCoreUtils.removeQuotes(sgsn.trim),
        status = EdmCoreUtils.removeQuotes(status.trim),
        tilt = EdmCoreUtils.parseDouble(EdmCoreUtils.removeQuotes(tilt)).getOrElse(DefaultTilt),
        cellType = EdmCoreUtils.removeQuotes(cellType.trim),
        shape = shapeText,
        lat = latitude,
        long = longitude,
        codeDesc = EgCellCodeDesc(
          vendor = EdmCoreUtils.removeQuotes(vendor.trim),
          btsCodeType = EdmCoreUtils.removeQuotes(btsCodeType.trim),
          btsCodeId = EdmCoreUtils.removeQuotes(btsCodeId.trim),
          btsCodeValue = EdmCoreUtils.removeQuotes(btsCodeValue.trim),
          btsCodeDescription = EdmCoreUtils.removeQuotes(btsCodeDescription.trim),
          btsStatusId = EdmCoreUtils.removeQuotes(btsStatusId.trim),
          btsStatusCode = EdmCoreUtils.removeQuotes(btsStatusCode.trim),
          btsStatusName1 = EdmCoreUtils.removeQuotes(btsStatusName1.trim),
          btsStatusName2 = EdmCoreUtils.removeQuotes(btsStatusName2.trim),
          cellVendorId = EdmCoreUtils.removeQuotes(cellVendorId.trim),
          cellVendorCode = EdmCoreUtils.removeQuotes(cellVendorCode.trim),
          cellVendorName1 = EdmCoreUtils.removeQuotes(cellVendorName1.trim),
          cellVendorName2 = EdmCoreUtils.removeQuotes(cellVendorName2.trim)))
    }
  }

  private def latLongFromGeomBlob(geomBlobText: String): (Double, Double) = {
    val args = geomBlobText.split(",")
    (EdmCoreUtils.parseDouble(args(3).trim).getOrElse(Double.NaN),
      EdmCoreUtils.parseDouble(args(2).trim).getOrElse(Double.NaN))
  }
}
