/*
 * TODO: License goes here!
 */

package sa.com.mobily.cell

import com.vividsolutions.jts.geom.Geometry

import sa.com.mobily.geometry._
import sa.com.mobily.parsing.{OpenCsvParser, CsvParser}

/** Technology of the cell */
sealed trait Technology { val identifier: String }

case object TwoG extends Technology { override val identifier = "2G" }
case object ThreeG extends Technology { override val identifier = "3G" }
case object FourGTdd extends Technology { override val identifier = "4G_TDD" }
case object FourGFdd extends Technology { override val identifier = "4G_FDD" }

/** Type of cell */
sealed trait CellType { val value: String }

case object Rdu extends CellType { override val value = "RDU" }
case object Crane extends CellType { override val value = "CRANE" }
case object Macro extends CellType { override val value = "MACRO" }
case object Rt extends CellType { override val value = "RT" }
case object Micro extends CellType { override val value = "MICRO" }
case object Tower extends CellType { override val value = "TOWER" }
case object Indoor extends CellType { override val value = "INDOOR" }
case object Monopole extends CellType { override val value = "MONOPOLE" }
case object Rds extends CellType { override val value = "RDS" }
case object PalmTree extends CellType { override val value = "PALM TREE" }
case object Outlet extends CellType { override val value = "OUTLET" }
case object Parking extends CellType { override val value = "PARKING" }
case object Pico extends CellType { override val value = "PICO" }

/** Cell information */
case class Cell(
    cellId: Int,
    lacTac: Int,
    planarCoords: UtmCoordinates,
    technology: Technology,
    cellType: CellType,
    height: Double,
    azimuth: Double,
    beamwidth: Double,
    range: Double,
    bts: String,
    coverageWkt: String,
    mcc: String = Cell.SaudiArabiaMcc,
    mnc: String = Cell.MobilyMnc) {

  lazy val coverageGeom: Geometry = GeomUtils.parseWkt(coverageWkt, planarCoords.srid)

  lazy val identifier: (Int, Int) = (lacTac, cellId)

  def centroidDistance(location: Geometry): Double = coverageGeom.getCentroid.distance(location.getCentroid)

  def areaRatio(location: Geometry): Double = coverageGeom.getArea / location.getArea

  def intersects(another: Cell): Boolean = coverageGeom.intersects(another.coverageGeom)

  def fields: Array[String] = {
    val coverageWktWgs84 =
      GeomUtils.wkt(
        GeomUtils.transformGeom(
          coverageGeom,
          Coordinates.Wgs84GeodeticSrid,
          Coordinates.LatLongPrecisionModel,
          true)) // Use longitude first (format recognized by QGIS and PostGIS, but it's not standard!)
    Array[String](
      mcc,
      mnc,
      cellId.toString,
      lacTac.toString,
      planarCoords.x.toString,
      planarCoords.y.toString,
      planarCoords.epsg,
      technology.identifier,
      cellType.value,
      height.toString,
      azimuth.toString,
      beamwidth.toString,
      range.toString,
      bts,
      coverageWkt,
      coverageWktWgs84)
  }
}

object Cell {

  val MccStartIndex = 0
  val MncStartIndex = 3
  val LacStartIndexMnc2Digits = 5
  val LacStartIndexMnc3Digits = 6

  private val SaudiArabiaMcc = "420"
  private val MobilyMnc = "03"

  final val lineCsvParserObject = new OpenCsvParser

  implicit val fromCsv = new CsvParser[Cell] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): Cell = {
      val Array(mccText, mncText, cellIdText, lacTacText, planarXText, planarYText, utmEpsg, techText, cellTypeText,
        heightText, azimuthText, beamwidthText, rangeText, btsText, geomText, _) = fields

      Cell(
        cellId = cellIdText.toInt,
        lacTac = lacTacText.toInt,
        planarCoords = UtmCoordinates(planarXText.toDouble, planarYText.toDouble, utmEpsg),
        technology = parseTechnology(techText),
        cellType = parseCellType(cellTypeText),
        height = heightText.toDouble,
        azimuth = azimuthText.toDouble,
        beamwidth = beamwidthText.toDouble,
        range = rangeText.toDouble,
        bts = btsText,
        coverageWkt = geomText,
        mcc = mccText,
        mnc = mncText)
    }
  }

  def parseTechnology(techText: String): Technology = techText.trim.toUpperCase match {
    case TwoG.identifier => TwoG
    case ThreeG.identifier => ThreeG
    case FourGFdd.identifier => FourGFdd
    case FourGTdd.identifier => FourGTdd
  }

  def parseCellType(cellTypeText: String): CellType = // scalastyle:ignore cyclomatic.complexity
    cellTypeText.trim.toUpperCase match {
      case Rdu.value => Rdu
      case Crane.value => Crane
      case Macro.value => Macro
      case Rt.value => Rt
      case Micro.value => Micro
      case Tower.value => Tower
      case Indoor.value => Indoor
      case Monopole.value => Monopole
      case Rds.value => Rds
      case PalmTree.value => PalmTree
      case Outlet.value => Outlet
      case Parking.value => Parking
      case Pico.value => Pico
    }
}
