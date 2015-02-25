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

  val Header: Array[String] = User.Header ++ Array("poiType", "geomWkt", "countryIsoCode")

  implicit val fromRow = new RowParser[Poi] {

    override def fromRow(row: Row): Poi = {
      val user = User.fromRow.fromRow(row(0).asInstanceOf[Row])
      val poiType = PoiType(row(1).asInstanceOf[Row].getString(0))
      val geomWkt = row.getString(2)
      val countryIsoCode = row.getString(3)

      Poi(user = user, poiType = poiType, geomWkt = geomWkt, countryIsoCode = countryIsoCode)
    }
  }
}
