/*
 * TODO: License goes here!
 */

package sa.com.mobily.geometry

import org.scalatest._

import sa.com.mobily.utils.EdmCustomMatchers

class CoordinatesTest extends FlatSpec with ShouldMatchers {

  trait WithLatLongCoordinates {

    val latLongCoords = LatLongCoordinates(24.7118352, 46.673837) // Mobily HQ
    val polarSrid = 4326
  }

  trait WithUtmCoordinates {

    val utmCoords = UtmCoordinates(669314, 2734074.4)
    val utmCoordsGeom = GeomUtils.parseWkt("POINT ( 669314 2734074.4 )", utmCoords.srid)
    val planarSrid = 32638
  }

  "UtmCoordinates" should "transform to LatLongCoordinates (WGS84 geodetic)" in new WithUtmCoordinates
      with WithLatLongCoordinates {
    utmCoords.latLongCoordinates() should be (latLongCoords)
  }

  it should "require valid numbers as coordinates" in {
    an [Exception] should be thrownBy UtmCoordinates("NaN".toDouble, 123.456)
    an [Exception] should be thrownBy UtmCoordinates(123.456, "NaN".toDouble)
  }

  it should "get the SRID" in new WithUtmCoordinates {
    utmCoords.srid should be (planarSrid)
  }

  it should "get the associated geometry (Point)" in new WithUtmCoordinates with EdmCustomMatchers {
    utmCoords.geometry should equalGeometry (utmCoordsGeom)
  }

  "LatLongCoordinates" should "transform to planar UtmCoordinates (WGS84 datum)" in new WithLatLongCoordinates
      with WithUtmCoordinates {
    latLongCoords.utmCoordinates() should be (utmCoords)
  }

  it should "require valid numbers as coordinates" in {
    an [Exception] should be thrownBy LatLongCoordinates("NaN".toDouble, 123.456)
    an [Exception] should be thrownBy LatLongCoordinates(123.456, "NaN".toDouble)
  }

  it should "get the SRID" in new WithLatLongCoordinates {
    latLongCoords.srid should be (polarSrid)
  }
}
