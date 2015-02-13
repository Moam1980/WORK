/*
 * TODO: License goes here!
 */

package sa.com.mobily.poi

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql._

import sa.com.mobily.geometry.{Coordinates, GeomUtils}
import sa.com.mobily.parsing.RowParser
import sa.com.mobily.roaming.CountryCode
import sa.com.mobily.user.User

case class Poi(user: User, poiType: PoiType, geomWkt: String, countryIsoCode: String = CountryCode.SaudiArabiaIsoCode) {

  lazy val geometry: Geometry = GeomUtils.parseWkt(geomWkt, Coordinates.isoCodeUtmSrid(countryIsoCode))

  def fields: Array[String] = user.fields ++ Array(poiType.identifier, geomWkt, countryIsoCode)
}

object Poi {

  def header: Array[String] = User.header ++ Array("poiType", "geomWkt", "countryIsoCode")

  implicit val fromRow = new RowParser[Poi] {

    override def fromRow(row: Row): Poi = {
      val user = User.fromRow.fromRow(row(0).asInstanceOf[Row])
      val poiType = row(1).asInstanceOf[PoiType]
      val geomWkt = row(2).asInstanceOf[String]
      val countryIsoCode = row(3).asInstanceOf[String]

      Poi(
        user = user,
        poiType = poiType,
        geomWkt = geomWkt,
        countryIsoCode = countryIsoCode)
    }
  }
}
