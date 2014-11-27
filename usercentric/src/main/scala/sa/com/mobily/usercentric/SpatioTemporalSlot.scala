/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import sa.com.mobily.cell.Cell
import sa.com.mobily.event.Event
import sa.com.mobily.geometry.GeomUtils
import sa.com.mobily.roaming.CountryCode

case class SpatioTemporalSlot(
    userId: Long,
    startTime: Long,
    endTime: Long,
    geomWkt: String,
    events: List[Event],
    countryIsoCode: String = CountryCode.SaudiArabiaIsoCode) extends CountryGeometry {

  lazy val cells: Set[(Int, Int)] = events.map(event => (event.lacTac, event.cellId)).toSet

  def append(event: Event)(implicit cellCatalogue: Map[(Int, Int), Cell]): SpatioTemporalSlot =
    SpatioTemporalSlot(
      userId = userId,
      startTime = startTime,
      endTime = event.endTime,
      geomWkt = GeomUtils.wkt(geom.intersection(cellCatalogue((event.lacTac, event.cellId)).coverageGeom)),
      events = events :+ event)
}

object SpatioTemporalSlot {

  def apply(event: Event)(implicit cellCatalogue: Map[(Int, Int), Cell]): SpatioTemporalSlot =
    SpatioTemporalSlot(
      userId = event.id,
      startTime = event.beginTime,
      endTime = event.endTime,
      geomWkt = cellCatalogue((event.lacTac, event.cellId)).coverageWkt,
      events = List(event))
}
