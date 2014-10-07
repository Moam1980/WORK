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

  def latLongCoordinates(destGeodeticEpsg: String = Coordinates.Wgs84GeodeticEpsg): LatLongCoordinates = {
    val geomFactory = new GeometryFactory
    val mathTransform = CRS.findMathTransform(CRS.decode(epsg), CRS.decode(destGeodeticEpsg))
    JTS.transform(geomFactory.createPoint(new Coordinate(x, y)), mathTransform) match {
      case p: Point => LatLongCoordinates(p.getX, p.getY, destGeodeticEpsg)
    }
  }

  def roundCoords: UtmCoordinates = UtmCoordinates(EdmCoreUtils.roundAt1(x), EdmCoreUtils.roundAt1(y), epsg)

  def srid: Int = epsg.substring(epsg.indexOf(Coordinates.AuthoritySridSep) + 1).toInt

  def geometry: Point = {
    val geomFactory = new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), srid)
    geomFactory.createPoint(new Coordinate(x, y))
  }
}

/** Point in geodetic coordinate system (WGS84 ellipsoid) - EPSG:4326
  *
  * @param lat angular distance in decimal degrees to the equator of the ellipsoid
  * @param long angular distance in decimal degrees to the reference meridian of the ellipsoid
  */
case class LatLongCoordinates(lat: Double, long: Double, epsg: String = Coordinates.Wgs84GeodeticEpsg) {

  def utmCoordinates(destUtmEpsg: String = Coordinates.SaudiArabiaUtmEpsg): UtmCoordinates = {
    val geomFactory = new GeometryFactory
    val mathTransform = CRS.findMathTransform(CRS.decode(epsg), CRS.decode(destUtmEpsg))
    JTS.transform(geomFactory.createPoint(new Coordinate(lat, long)), mathTransform) match {
      case p: Point => UtmCoordinates(p.getX, p.getY, destUtmEpsg)
    }
  }
}

object Coordinates {

  val SaudiArabiaUtmEpsg = "EPSG:32638" // UTM zone 38N
  val Wgs84GeodeticEpsg = "EPSG:4326"
  val AuthoritySridSep = ":"
}
