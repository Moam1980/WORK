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
    cgi: String,
    planarCoords: UtmCoordinates,
    technology: Technology,
    cellType: CellType,
    height: Double,
    azimuth: Double,
    tilt: Double,
    power: Double,
    beamwidth: Double,
    coverageWkt: String) {

  require(cgi.length == Cell.CgiLength)

  lazy val mcc: String = cgi.substring(Cell.MccStartIndex, Cell.MncStartIndex)

  lazy val mnc: String = cgi.substring(Cell.MncStartIndex, Cell.LacStartIndex)

  lazy val lac: String = technology match {
    case TwoG => cgi.substring(Cell.LacStartIndex, Cell.CellIdStartIndex2g3g)
    case ThreeG => cgi.substring(Cell.LacStartIndex, Cell.CellIdStartIndex2g3g)
    case _ => cgi.substring(Cell.LacStartIndex, Cell.CellIdStartIndex4g)
  }

  lazy val cellId: String = technology match {
    case TwoG => cgi.substring(Cell.CellIdStartIndex2g3g)
    case ThreeG => cgi.substring(Cell.CellIdStartIndex2g3g)
    case _ => cgi.substring(Cell.CellIdStartIndex4g)
  }

  lazy val coverageGeom: Geometry = GeomUtils.parseWkt(coverageWkt, planarCoords.srid)
}

object Cell {

  val CgiLength = 15
  val MccStartIndex = 0
  val MncStartIndex = 3
  val LacStartIndex = 6
  val CellIdStartIndex2g3g = 10
  val CellIdStartIndex4g = 11

  final val lineCsvParserObject = new OpenCsvParser

  implicit val fromCsv = new CsvParser[Cell] {

    override def lineCsvParser: OpenCsvParser = lineCsvParserObject

    override def fromFields(fields: Array[String]): Cell = {
      val (cellInfo, cellGeometry) = fields.splitAt(10) // scalastyle:ignore magic.number
      require(cellGeometry.isEmpty || (cellGeometry.length == 1))
      val Array(cgiText, latText, longText, techText, cellTypeText, heightText,
        azimuthText, tiltText, powerText, beamwidthText) = cellInfo

      val cgi = parseCgi(cgiText)
      val coords = LatLongCoordinates(latText.toDouble, longText.toDouble).utmCoordinates()
      val tech = parseTechnology(techText)
      val cellType = parseCellType(cellTypeText)
      val height = heightText.toDouble
      val azimuth = azimuthText.toDouble
      val tilt = tiltText.toDouble
      val power = powerText.toDouble
      val beamwidth = beamwidthText.toDouble
      val coverageWkt =
        if (cellGeometry.isEmpty)
          GeomUtils.wkt(
            CellCoverage.cellShape(
              cellLocation = coords.geometry,
              height = height,
              azimuth = azimuth,
              beamwidth = beamwidth,
              tilt = tilt,
              technology = tech))
        else cellGeometry.head

      Cell(cgi, coords, tech, cellType, height, azimuth, tilt, power, beamwidth, coverageWkt)
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

  private def parseCgi(cgiText: String) = {
    val trimmedCgi = cgiText.trim
    require(trimmedCgi.length == 15)
    trimmedCgi
  }
}
