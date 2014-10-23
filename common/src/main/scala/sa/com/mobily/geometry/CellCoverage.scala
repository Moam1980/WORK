/*
 * TODO: License goes here!
 */

package sa.com.mobily.geometry

import scala.math._

import com.vividsolutions.jts.geom.{Point, Geometry}

import sa.com.mobily.cell._

object CoverageModel extends Enumeration {

  type CoverageModel = Value
  val CircularSectors, Petals = Value
}

/** Utils for building the cell coverage polygon from its parameters */
object CellCoverage {

  import CoverageModel._

  val OutdoorBandOffset2g = 1.4
  val OutdoorBandOffset3g = 1
  val OutdoorBandOffset4gTdd = 0.8
  val OutdoorBandOffset4gFdd = 1
  val DefaultBackLobeRatio = 0.1 // with respect to cell range

  def cellShape(
      cellLocation: Point,
      azimuth: Double,
      beamwidth: Double,
      range: Double,
      coverageModel: CoverageModel = Petals): Geometry = {
    if (beamwidth < 360) {
      // Directional antenna
      val mainLobe = coverageModel match {
        case Petals if beamwidth < 180 =>
          GeomUtils.hippopede(location = cellLocation, radius = range, azimuth = azimuth, beamwidth = beamwidth)
        case Petals if beamwidth >= 180 =>
          GeomUtils.conchoid(location = cellLocation, radius = range, azimuth = azimuth, beamwidth = beamwidth)
        case CircularSectors =>
          GeomUtils.circularSector(position = cellLocation, azimuth = azimuth, beamwidth = beamwidth, radius = range)
      }
      GeomUtils.addBackLobe(mainLobe, cellLocation, range, DefaultBackLobeRatio)
    } else {
      // Omnidirectional antenna
      GeomUtils.circle(cellLocation, range)
    }
  }

  /** Maximum reach of the cell coverage
    *
    * TODO: This formula must be reconsidered and validated (through flickering analysis and on the field data samples)
    */
  def cellRange(tilt: Double, height: Double, technology: Technology): Double = {
    val bandOffset = technology match {
      case TwoG => OutdoorBandOffset2g
      case ThreeG => OutdoorBandOffset3g
      case FourGFdd => OutdoorBandOffset4gFdd
      case FourGTdd => OutdoorBandOffset4gTdd
    }
    tan(toRadians(90 - tilt)) * (height * bandOffset)
  }
}
