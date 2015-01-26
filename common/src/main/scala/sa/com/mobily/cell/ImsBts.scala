/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import sa.com.mobily.parsing.{CsvParser, OpenCsvParser}
import sa.com.mobily.roaming.CountryCode
import sa.com.mobily.utils.EdmCoreUtils

case class ImsBts(
    vendor: String,
    technology: Technology,
    id: String,
    nodeB: String,
    parentBsc: String,
    longitude: String,
    latitude: String,
    ipAddress: String,
    netmask: String,
    lac: Option[Int],
    mcc: String = CountryCode.SaudiArabiaMcc,
    mnc: String = CountryCode.MobilyMnc) {

  def fields: Array[String] =
    Array(vendor, technology.identifier, id, nodeB, parentBsc, longitude, latitude, ipAddress, netmask,
      lac.getOrElse("").toString, mcc, mnc)
}

object ImsBts {

  def header: Array[String] =
    Array("vendor", "technology", "id", "nodeB", "parentBsc", "longitude", "latitude", "ipAddress", "netmask",
      "lac", "mcc", "mnc")

  final val lineCsvParserObject = new OpenCsvParser(quote = '"')
  private val CompositeNodeB = "_G12-TG-"

  implicit val fromCsv = new CsvParser[ImsBts] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): ImsBts = {
      val Array(vendorText, technologyText, nodeBText, parentBscText, longitudeText, latitudeText, ipAddressText,
        netmaskText, lacText, mccText, mncText) = fields

      ImsBts(
        vendor = vendorText,
        technology = Cell.parseTechnology(technologyText),
        id = parseIdFromNodeB(nodeBText),
        nodeB = nodeBText,
        parentBsc = parentBscText,
        longitude = EdmCoreUtils.parseNullString(longitudeText),
        latitude = EdmCoreUtils.parseNullString(latitudeText),
        ipAddress = EdmCoreUtils.parseNullString(ipAddressText),
        netmask = EdmCoreUtils.parseNullString(netmaskText),
        lac = EdmCoreUtils.parseInt(lacText),
        mcc = EdmCoreUtils.parseNullString(mccText),
        mnc = EdmCoreUtils.parseNullString(mncText))
    }
  }

  def parseIdFromNodeB(nodeBText: String): String =
    if (nodeBText.trim.contains(CompositeNodeB)) {
      val id = nodeBText.substring(0, nodeBText.trim.indexOf(CompositeNodeB)).split("_").last
      if (id.startsWith("G")) id.substring(1)
      else id
    } else nodeBText

  def merge(imsBts1: ImsBts, imsBts2: ImsBts): ImsBts = {
    imsBts1.copy(
      longitude = if (imsBts1.longitude.isEmpty) imsBts2.longitude else imsBts1.longitude,
      latitude = if (imsBts1.latitude.isEmpty) imsBts2.latitude else imsBts1.latitude,
      ipAddress = if (imsBts1.ipAddress.isEmpty) imsBts2.ipAddress else imsBts1.ipAddress,
      netmask = if (imsBts1.netmask.isEmpty) imsBts2.netmask else imsBts1.netmask,
      lac = if (!imsBts1.lac.isDefined) imsBts2.lac else imsBts1.lac,
      mcc = if (imsBts1.mcc.isEmpty) imsBts2.mcc else imsBts1.mcc,
      mnc = if (imsBts1.mnc.isEmpty) imsBts2.mnc else imsBts1.mnc)
  }
}
