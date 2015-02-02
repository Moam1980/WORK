/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import org.scalatest._

import sa.com.mobily.parsing.CsvParser

class ImsCellTest extends FlatSpec with ShouldMatchers {

  import ImsCell._

  trait WithImsCell {

    val imsCellLine = "\"ALU\"|\"2G\"|\"AJH3440\"|\"NULL\"|\"NULL\"|\"E371_ARAR3\"|\"AJH3440_1\"|\"34401\"|\"NULL\"|" +
      "\"NULL\"|\"3833\"|\"NULL\"|\"NULL\"|\"420\"|\"03\"|\"40\"|\"24\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|" +
      "\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\"|\"NULL\""

    val fields = Array("ALU", "2G", "AJH3440", "NULL", "NULL", "E371_ARAR3", "AJH3440_1", "34401", "NULL", "NULL",
      "3833", "NULL", "NULL", "420", "03", "40", "24", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL",
      "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL")

    val imsCellIdentifierHeader = Array("cellId", "lac", "sac", "rac", "tac", "mcc", "mnc")
    val imsCellHeader =
      ImsBts.header ++
        ImsCellIdentifier.header ++
        Array("cellName", "cellType", "cSysType", "sector", "sectorBranch", "antennaType", "bcchFrequency",
          "maxTxPower", "altitude", "minAltitude", "envChar", "height", "talim", "azimuth", "tilt", "beamWith",
          "accMin", "primarySchPower", "qRxLevMin")

    val imsBtsFields = Array("ALU", "2G", "AJH3440", "AJH3440", "E371_ARAR3", "", "", "", "")
    val imsCellIdentifierFields = Array("34401", "3833", "", "", "", "420", "03")
    val imsCellFields =
      imsBtsFields ++ imsCellIdentifierFields ++
      Array("AJH3440_1", "", "", "", "", "", "40.0", "24.0", "", "", "", "", "", "", "", "", "", "", "")

    val imsBts = ImsBts("ALU", TwoG, "AJH3440", "E371_ARAR3", "", "", "", "")
    val imsCellIdentifier = ImsCellIdentifier("34401", Some(3833), None, None, None, "420", "03")

    val imsCell = ImsCell(
      imsBts = imsBts,
      imsCellId = imsCellIdentifier,
      cellName = "AJH3440_1",
      cellType = None,
      cSysType = None,
      sector = "",
      sectorBranch = "",
      antennaType = "",
      bcchFrequency = Some(40),
      maxTxPower = Some(24),
      altitude = None,
      minAltitude = None,
      envChar = "",
      height = None,
      talim = None,
      azimuth = None,
      tilt = None,
      beamWith = None,
      accMin = None,
      primarySchPower = None,
      qRxLevMin = None)
  }

  trait WithImsCellAllFields {

    val imsBtsFields =
      Array("ALU", "3G", "AJH3440", "AJH3440", "E371_ARAR3", "longitude", "latitude", "ipAddress", "netmask")
    val imsCellIdentifierFields = Array("34401", "3833", "1", "3833", "", "420", "03")
    val imsCellFields =
      imsBtsFields ++ imsCellIdentifierFields ++
        Array("AJH3440_1", "MACRO", "GSM900", "sector", "sectorBranch", "antennaType", "40.1", "24.2", "1.3", "2.4",
          "envChar", "3.5", "4.6", "5.7", "6.8", "7.9", "8.1", "9.11", "10.12")

    val imsBts = ImsBts("ALU", ThreeG, "AJH3440", "E371_ARAR3", "longitude", "latitude", "ipAddress", "netmask")
    val imsCellIdentifier = ImsCellIdentifier("34401", Some(3833), Some(1), Some(3833), None, "420", "03")

    val imsCell = ImsCell(
      imsBts = imsBts,
      imsCellId = imsCellIdentifier,
      cellName = "AJH3440_1",
      cellType = Some(Macro),
      cSysType = Some(Gsm900),
      sector = "sector",
      sectorBranch = "sectorBranch",
      antennaType = "antennaType",
      bcchFrequency = Some(40.1),
      maxTxPower = Some(24.2),
      altitude = Some(1.3),
      minAltitude = Some(2.4),
      envChar = "envChar",
      height = Some(3.5),
      talim = Some(4.6),
      azimuth = Some(5.7),
      tilt = Some(6.8),
      beamWith = Some(7.9),
      accMin = Some(8.10),
      primarySchPower = Some(9.11),
      qRxLevMin = Some(10.12))

    val imsBtsFields4G =
      Array("ALU", "4G_TDD", "AJH3440", "AJH3440", "E371_ARAR3", "longitude", "latitude", "ipAddress", "netmask")
    val imsCellIdentifierFields4G = Array("34401", "", "", "", "3333", "420", "03")
    val imsCellFields4G =
      imsBtsFields4G ++ imsCellIdentifierFields4G ++
        Array("AJH3440_1", "MACRO", "GSM900", "sector", "sectorBranch", "antennaType", "40.1", "24.2", "1.3", "2.4",
          "envChar", "3.5", "4.6", "5.7", "6.8", "7.9", "8.1", "9.11", "10.12")

    val imsBts4G = imsBts.copy(technology = FourGTdd)
    val imsCellIdentifier4G = imsCellIdentifier.copy(lac = None, sac = None, rac = None, tac = Some(3333))
    val imsCell4G = imsCell.copy(imsBts = imsBts4G, imsCellId = imsCellIdentifier4G)
  }

  "ImsCell" should "return correct header for cell identifier" in new WithImsCell {
    ImsCellIdentifier.header should be (imsCellIdentifierHeader)
  }

  it should "return correct header for cell" in new WithImsCell {
    ImsCell.header should be (imsCellHeader)
  }

  it should "return correct fields for cell identifier" in new WithImsCell {
    imsCellIdentifier.fields should be (imsCellIdentifierFields)
  }

  it should "return correct fields for cell" in new WithImsCell {
    imsCell.fields should be (imsCellFields)
  }

  it should "return correct fields for cell complete" in new WithImsCellAllFields {
    imsCell.fields should be (imsCellFields)
  }

  it should "return correct fields for cell 4G" in new WithImsCellAllFields {
    imsCell4G.fields should be (imsCellFields4G)
  }

  it should "be built from CSV" in new WithImsCell {
    CsvParser.fromLine(imsCellLine).value.get should be (imsCell)
  }

  it should "be discarded when the CSV format is wrong" in new WithImsCell {
    an [Exception] should be thrownBy fromCsv.fromFields(fields.updated(1, "NotTechnology"))
  }

  it should "parse cell type empty as none" in {
    ImsCell.parseCellType("") should be (None)
  }

  it should "parse cell type null as none" in {
    ImsCell.parseCellType("NULL") should be (None)
  }

  it should "parse cell sys type GSM900" in {
    ImsCell.parseCellSysType("GSM900") should be (Some(Gsm900))
  }

  it should "parse cell sys type GSM1800" in {
    ImsCell.parseCellSysType("GSM1800") should be (Some(Gsm1800))
  }

  it should "parse cell sys type empty as none" in {
    ImsCell.parseCellSysType("") should be (None)
  }

  it should "parse cell sys type null as none" in {
    ImsCell.parseCellSysType("NULL") should be (None)
  }
}
