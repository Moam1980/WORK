/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import scala.util.Try

import sa.com.mobily.parsing.{OpenCsvParser, CsvParser}
import sa.com.mobily.roaming.CountryCode
import sa.com.mobily.utils.EdmCoreUtils

sealed trait CellSysType { val identifier: String }

case object Gsm900 extends CellSysType { override val identifier = "GSM900" }
case object Gsm1800 extends CellSysType { override val identifier = "GSM1800" }

case class ImsCellIdentifier(
    cellId: String,
    lac: Option[Int],   // Location Area Code -- group Bts used in CS events
    sac: Option[Int],   // Service Area Code -- subdivision of LAC
                        // The SA identifies an area of one or more cells of the same LA (Location Area).
                        // And is used to indicate the location of a UE (User Equipment) to the CN (Core Network).
                        // The combination of SAC (Service Area Code), PLMN -Id (Public Land Mobile Network Identifier)
                        // and LAC (LocationArea Code) is the Service Area Identifier.
                        // SAI = PLMN-Id + LAC + SAC
    rac: Option[Int],   // Routing Area Code -- group Bts used in PS events, a LAC normally have from 2 to 6 RAC
    tac: Option[Int],   // Tracking Area Code -- same as RAC for in LTE/4G
    mcc: String = CountryCode.SaudiArabiaMcc,
    mnc: String = CountryCode.MobilyMnc) {

  def fields: Array[String] =
    Array(
      cellId,
      lac.getOrElse("").toString,
      sac.getOrElse("").toString,
      rac.getOrElse("").toString,
      tac.getOrElse("").toString,
      mcc,
      mnc)
}

object ImsCellIdentifier {

  val Header: Array[String] = Array("cellId", "lac", "sac", "rac", "tac", "mcc", "mnc")
}

case class ImsCell(
    imsBts: ImsBts,
    imsCellId: ImsCellIdentifier,
    cellName: String,
    cellType: Option[CellType],
    cSysType: Option[CellSysType],
    sector: String,
    sectorBranch: String,
    antennaType: String,
    bcchFrequency: Option[Double],
    maxTxPower: Option[Double],
    altitude: Option[Double],
    minAltitude: Option[Double],
    envChar: String,
    height: Option[Double],
    talim: Option[Double],            // Configuration of antenna for maximum time advance
    azimuth: Option[Double],
    tilt: Option[Double],
    beamWith: Option[Double],
    accMin: Option[Double],
    primarySchPower: Option[Double],
    qRxLevMin: Option[Double]) {        // Minimum required receive level in this cell, given in dBm

  def fields: Array[String] =
    imsBts.fields ++
      imsCellId.fields ++
      Array(
        cellName,
        if (cellType.isDefined) cellType.get.value else "",
        if (cSysType.isDefined) cSysType.get.identifier else "",
        sector,
        sectorBranch,
        antennaType,
        bcchFrequency.getOrElse("").toString,
        maxTxPower.getOrElse("").toString,
        altitude.getOrElse("").toString,
        minAltitude.getOrElse("").toString,
        envChar,
        height.getOrElse("").toString,
        talim.getOrElse("").toString,
        azimuth.getOrElse("").toString,
        tilt.getOrElse("").toString,
        beamWith.getOrElse("").toString,
        accMin.getOrElse("").toString,
        primarySchPower.getOrElse("").toString,
        qRxLevMin.getOrElse("").toString)
}

object ImsCell {

  val Header: Array[String] =
    ImsBts.Header ++ ImsCellIdentifier.Header ++
    Array("cellName", "cellType", "cSysType", "sector", "sectorBranch", "antennaType", "bcchFrequency", "maxTxPower",
      "altitude", "minAltitude", "envChar", "height", "talim", "azimuth",  "tilt", "beamWith", "accMin",
      "primarySchPower", "qRxLevMin")

  final val lineCsvParserObject = new OpenCsvParser(quote = '"')

  implicit val fromCsv = new CsvParser[ImsCell] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): ImsCell = {
      val (firstChunk, secondChunk) = fields.splitAt(22) // scalastyle:ignore magic.number
      val Array(vendorText, technologyText, nodeText, ipAddressText, netmaskText, parentBscText, cellNameText,
        cellIdText, longitudeText, latitudeText, lacText, racText, sacText, mccText, mncText, bcchFrequencyText,
        maxTxPowerText, cellTypeText, cSysTypeText, altitudeText, minAltitudeText, envCharText) = firstChunk
      val Array(heightText, talimText, azimuthText, sectorText, sectorBranchText, tiltText, beamWithText, accMinText,
        antennaTypeText, primarySchPowerText, qRxLevMinText) = secondChunk

      ImsCell(
        imsBts = ImsBts(
          vendor = vendorText,
          technology = Cell.parseTechnology(technologyText),
          node = EdmCoreUtils.parseNullString(nodeText),
          parentBsc = EdmCoreUtils.parseNullString(parentBscText),
          longitude = EdmCoreUtils.parseNullString(longitudeText),
          latitude = EdmCoreUtils.parseNullString(latitudeText),
          ipAddress = EdmCoreUtils.parseNullString(ipAddressText),
          netmask = EdmCoreUtils.parseNullString(netmaskText)),
        imsCellId = ImsCellIdentifier(
          cellId = cellIdText,
          lac = EdmCoreUtils.parseInt(EdmCoreUtils.parseNullString(lacText)),
          sac = EdmCoreUtils.parseInt(EdmCoreUtils.parseNullString(sacText)),
          rac = EdmCoreUtils.parseInt(EdmCoreUtils.parseNullString(racText)),
          tac = None,
          mcc = EdmCoreUtils.parseNullString(mccText),
          mnc = EdmCoreUtils.parseNullString(mncText)),
        cellName = EdmCoreUtils.parseNullString(cellNameText),
        cellType = parseCellType(cellTypeText),
        cSysType = parseCellSysType(cSysTypeText),
        sector = EdmCoreUtils.parseNullString(sectorText),
        sectorBranch = EdmCoreUtils.parseNullString(sectorBranchText),
        antennaType = EdmCoreUtils.parseNullString(antennaTypeText),
        bcchFrequency = EdmCoreUtils.parseDouble(EdmCoreUtils.parseNullString(bcchFrequencyText)),
        maxTxPower = EdmCoreUtils.parseDouble(EdmCoreUtils.parseNullString(maxTxPowerText)),
        altitude = EdmCoreUtils.parseDouble(EdmCoreUtils.parseNullString(altitudeText)),
        minAltitude = EdmCoreUtils.parseDouble(EdmCoreUtils.parseNullString(minAltitudeText)),
        envChar = EdmCoreUtils.parseNullString(envCharText),
        height = EdmCoreUtils.parseDouble(EdmCoreUtils.parseNullString(heightText)),
        talim = EdmCoreUtils.parseDouble(EdmCoreUtils.parseNullString(talimText)),
        azimuth = EdmCoreUtils.parseDouble(EdmCoreUtils.parseNullString(azimuthText)),
        tilt = EdmCoreUtils.parseDouble(EdmCoreUtils.parseNullString(tiltText)),
        beamWith = EdmCoreUtils.parseDouble(EdmCoreUtils.parseNullString(beamWithText)),
        accMin = EdmCoreUtils.parseDouble(EdmCoreUtils.parseNullString(accMinText)),
        primarySchPower = EdmCoreUtils.parseDouble(EdmCoreUtils.parseNullString(primarySchPowerText)),
        qRxLevMin = EdmCoreUtils.parseDouble(EdmCoreUtils.parseNullString(qRxLevMinText)))
    }
  }

  def parseCellType(cellTypeText: String): Option[CellType] = Try{ Cell.parseCellType(cellTypeText) }.toOption

  def parseCellSysType(cSysTypeText: String): Option[CellSysType] = cSysTypeText.trim.toUpperCase match {
    case Gsm900.identifier => Some(Gsm900)
    case Gsm1800.identifier => Some(Gsm1800)
    case _ => None
  }
}
