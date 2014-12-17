/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import com.vividsolutions.jts.geom.Geometry

import sa.com.mobily.geometry.{Coordinates, GeomUtils}
import sa.com.mobily.roaming.CountryCode
import sa.com.mobily.user.User

case class Poi(user: User, poiType: PoiType, geomWkt: String, countryIsoCode: String = CountryCode.SaudiArabiaIsoCode) {

  lazy val geometry: Geometry = GeomUtils.parseWkt(geomWkt, Coordinates.isoCodeUtmSrid(countryIsoCode))

  def fields: Array[String] = user.fields ++ Array(poiType.identifier, geomWkt, countryIsoCode)
}

object Poi {

  def header: Array[String] = User.header ++ Array("poiType", "geomWkt", "countryIsoCode")
}
