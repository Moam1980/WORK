/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import com.vividsolutions.jts.geom.Geometry

import sa.com.mobily.event.Event
import sa.com.mobily.geometry.{Coordinates, GeomUtils}
import sa.com.mobily.roaming.CountryCode

trait CountryGeometry {

  val geomWkt: String
  val countryIsoCode: String

  lazy val geom: Geometry = GeomUtils.parseWkt(geomWkt, Coordinates.isoCodeUtmSrid(countryIsoCode))
}

trait CellSequence {

  val orderedCells: List[(Int, Int)]

  lazy val cells: Set[(Int, Int)] = orderedCells.toSet
}

case class SpatioTemporalSlot(
    user: Long,
    startTime: Long,
    endTime: Long,
    geomWkt: String,
    events: List[Event],
    countryIsoCode: String = CountryCode.SaudiArabiaIsoCode) extends CountryGeometry {

  lazy val cells: Set[(Int, Int)] = events.map(event => (event.lacTac, event.cellId)).toSet
}
