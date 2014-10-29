/*
 * TODO: License goes here!
 */

package sa.com.mobily.utils

import com.vividsolutions.jts.geom.Geometry
import org.scalatest.matchers.{MatchResult, Matcher}

trait EdmCustomMatchers {

  case class EqualGeometry(expectedGeom: Geometry, tolerance: Double = 1e-2) extends Matcher[Geometry] {

    def apply(left: Geometry): MatchResult = {
      val typeMatch = left.getGeometryType == expectedGeom.getGeometryType
      val sridMatch = typeMatch && left.getSRID == expectedGeom.getSRID
      val geomCoords = left.getCoordinates
      val expectedCoords = expectedGeom.getCoordinates
      val numCoordsMatch = sridMatch && geomCoords.size == expectedCoords.size
      val sameCoordsMatch = numCoordsMatch && geomCoords.zip(expectedCoords).foldLeft(true) { (accum, coordPair) =>
        accum &&
          math.abs(coordPair._1.x - coordPair._2.x) < tolerance &&
          math.abs(coordPair._1.y - coordPair._2.y) < tolerance
      }
      MatchResult(
        sameCoordsMatch,
        s"Geometries are not equal at tolerance $tolerance ($left and $expectedGeom are not the same)",
        s"Geometries are equal at tolerance $tolerance ($left and $expectedGeom are the same)")
    }
  }

  def equalGeometry(expectedGeom: Geometry) = new EqualGeometry(expectedGeom)

  def equalGeometry(expectedGeom: Geometry, tolerance: Double) = new EqualGeometry(expectedGeom, tolerance)
}

object EdmCustomMatchers extends EdmCustomMatchers
