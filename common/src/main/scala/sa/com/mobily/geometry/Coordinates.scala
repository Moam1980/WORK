/*
 * TODO: License goes here!
 */

package sa.com.mobily.geometry

import com.vividsolutions.jts.geom._
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS

import sa.com.mobily.utils.EdmCoreUtils

/** Cartesian coordinates in the UTM (planar) coordinate system (WGS84 ellipsoid)
  *
  * @param x the x-axis coordinate in meters (longitude counterpart)
  * @param y the y-axis coordinate in meters (latitude counterpart)
  * @param epsg the EPSG coordinate reference system code (more info at http://www.epsg.org/)
  */
case class UtmCoordinates(
    x: Double,
    y: Double,
    epsg: String = Coordinates.SaudiArabiaUtmEpsg) {

  require(!x.isNaN && !y.isNaN)

  def latLongCoordinates(destGeodeticEpsg: String = Coordinates.Wgs84GeodeticEpsg): LatLongCoordinates = {
    val geomFactory = GeomUtils.geomFactory(srid, Coordinates.UtmPrecisionModel)
    val mathTransform = CRS.findMathTransform(CRS.decode(epsg), CRS.decode(destGeodeticEpsg))
    val coord = JTS.transform(geomFactory.createPoint(new Coordinate(x, y)), mathTransform).getCoordinate
    Coordinates.LatLongPrecisionModel.makePrecise(coord)
    LatLongCoordinates(coord.x, coord.y, destGeodeticEpsg)
  }

  def srid: Int = Coordinates.srid(epsg)

  def geometry: Point = GeomUtils.geomFactory(Coordinates.srid(epsg)).createPoint(new Coordinate(x, y))
}

/** Point in geodetic coordinate system (WGS84 ellipsoid) - EPSG:4326
  *
  * @param lat angular distance in decimal degrees to the equator of the ellipsoid
  * @param long angular distance in decimal degrees to the reference meridian of the ellipsoid
  */
case class LatLongCoordinates(lat: Double, long: Double, epsg: String = Coordinates.Wgs84GeodeticEpsg) {

  require(!lat.isNaN && !long.isNaN)

  def utmCoordinates(destUtmEpsg: String = Coordinates.SaudiArabiaUtmEpsg): UtmCoordinates = {
    val geomFactory = GeomUtils.geomFactory(srid, Coordinates.LatLongPrecisionModel)
    val mathTransform = CRS.findMathTransform(CRS.decode(epsg), CRS.decode(destUtmEpsg))
    val coord = JTS.transform(geomFactory.createPoint(new Coordinate(lat, long)), mathTransform).getCoordinate
    Coordinates.UtmPrecisionModel.makePrecise(coord)
    UtmCoordinates(coord.x, coord.y, destUtmEpsg)
  }

  def srid: Int = Coordinates.srid(epsg)
}

object Coordinates {

  val EpsgAuthority = "EPSG"
  val AuthoritySridSep = ":"
  val SaudiArabiaUtmSrid = 32638
  val Wgs84GeodeticSrid = 4326
  val SaudiArabiaUtmEpsg = epsg(SaudiArabiaUtmSrid) // UTM zone 38N
  val Wgs84GeodeticEpsg = epsg(Wgs84GeodeticSrid)
  val OneDecimalScale = 10
  val SevenDecimalsScale = 1e7
  val UtmPrecisionModel = new PrecisionModel(OneDecimalScale)
  val LatLongPrecisionModel = new PrecisionModel(SevenDecimalsScale)

  def srid(epsg: String): Int = epsg.substring(epsg.indexOf(Coordinates.AuthoritySridSep) + 1).toInt

  def epsg(srid: Int): String = s"$EpsgAuthority$AuthoritySridSep$srid"
}
