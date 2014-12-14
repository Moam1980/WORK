/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import sa.com.mobily.cell.Cell
import sa.com.mobily.event.Event
import sa.com.mobily.geometry.{Coordinates, GeomUtils}
import sa.com.mobily.roaming.CountryCode
import sa.com.mobily.utils.EdmCoreUtils

case class SpatioTemporalSlot(
    userId: Long,
    startTime: Long,
    endTime: Long,
    geomWkt: String,
    cells: Set[(Int, Int)],
    countryIsoCode: String = CountryCode.SaudiArabiaIsoCode,
    score: Option[CompatibilityScore] = None) extends CountryGeometry {

  def append(event: Event)(implicit cellCatalogue: Map[(Int, Int), Cell]): SpatioTemporalSlot =
    SpatioTemporalSlot(
      userId = userId,
      startTime = startTime,
      endTime = event.endTime,
      geomWkt = GeomUtils.wkt(geom.intersection(cellCatalogue((event.lacTac, event.cellId)).coverageGeom)),
      cells = cells + ((event.lacTac, event.cellId)),
      countryIsoCode = countryIsoCode)

  def append(slot: SpatioTemporalSlot): SpatioTemporalSlot =
    SpatioTemporalSlot(
      userId = userId,
      startTime = startTime,
      endTime = slot.endTime,
      geomWkt = GeomUtils.wkt(geom.intersection(slot.geom)),
      cells = cells ++ slot.cells,
      countryIsoCode = countryIsoCode)

  def fields: Array[String] =
    Array(
      userId.toString,
      EdmCoreUtils.fmt.print(startTime),
      EdmCoreUtils.fmt.print(endTime),
      cells.mkString(EdmCoreUtils.IntraSequenceSeparator),
      geomWkt,
      countryIsoCode)
}

object SpatioTemporalSlot {

  def apply(event: Event)(implicit cellCatalogue: Map[(Int, Int), Cell]): SpatioTemporalSlot =
    SpatioTemporalSlot(
      userId = event.id,
      startTime = event.beginTime,
      endTime = event.endTime,
      geomWkt = cellCatalogue((event.lacTac, event.cellId)).coverageWkt,
      cells = Set((event.lacTac, event.cellId)),
      countryIsoCode = Coordinates.utmSridIsoCode(cellCatalogue.head._2.planarCoords.srid))

  def header: Array[String] =
    Array("userId", "startTime", "endTime", "cells", "geomWkt", "countryIsoCode")
}
