/*
 * TODO: License goes here!
 */

package sa.com.mobily.geometry

import com.vividsolutions.jts.geom.Point
import org.scalatest._

class CoordinatesTest extends FlatSpec with ShouldMatchers {

  trait WithLatLongCoordinates {

    val latLongCoords = LatLongCoordinates(24.711835, 46.673837) // Mobily HQ
    val latLonTolerance = 1e-7
  }

  trait WithUtmCoordinates {

    val utmCoords = UtmCoordinates(669313.9971405969, 2734074.3794779177)
    val roundedUtmCoords = UtmCoordinates(669314.0, 2734074.4)
    val utmTolerance = 1e-2
    val srid = 32638
  }

  "UtmCoordinates" should "transform to LatLongCoordinates (WGS84 geodetic)" in new WithUtmCoordinates
      with WithLatLongCoordinates {
    val geodeticCoords = utmCoords.latLongCoordinates()
    geodeticCoords.long should be (latLongCoords.long +- latLonTolerance)
    geodeticCoords.lat should be (latLongCoords.lat +- latLonTolerance)
    geodeticCoords.epsg should be (Coordinates.Wgs84GeodeticEpsg)
  }

  it should "round to 1 decimal" in new WithUtmCoordinates {
    utmCoords.roundCoords should be (roundedUtmCoords)
  }

  it should "get the SRID" in new WithUtmCoordinates {
    utmCoords.srid should be (srid)
  }

  it should "get the associated geometry (Point)" in new WithUtmCoordinates {
    val geom = utmCoords.geometry
    geom shouldBe a [Point]
    geom.getX should be (utmCoords.x)
    geom.getY should be (utmCoords.y)
    geom.getSRID should be (utmCoords.srid)
  }

  "LatLongCoordinates" should "transform to planar UtmCoordinates (WGS84 datum)" in new WithLatLongCoordinates
      with WithUtmCoordinates {
    val planarCoords = latLongCoords.utmCoordinates()
    planarCoords.x should be (utmCoords.x +- utmTolerance)
    planarCoords.y should be (utmCoords.y +- utmTolerance)
    planarCoords.epsg should be (Coordinates.SaudiArabiaUtmEpsg)
  }
}
