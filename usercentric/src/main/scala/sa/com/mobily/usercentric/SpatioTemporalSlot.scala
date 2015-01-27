/*
 * TODO: License goes here!
 */

package sa.com.mobily.usercentric

import sa.com.mobily.cell.Cell
import sa.com.mobily.event.Event
import sa.com.mobily.geometry.{Coordinates, GeomUtils}
import sa.com.mobily.roaming.CountryCode
import sa.com.mobily.user.User
import sa.com.mobily.utils.EdmCoreUtils

/** User centric slot type estimate */
sealed case class ModelEntityEstimate(id: String)

object DwellEstimate extends ModelEntityEstimate(id = "Dwell")
object JourneyViaPointEstimate extends ModelEntityEstimate(id = "JourneyViaPoint")

case class SpatioTemporalSlot(
    user: User,
    startTime: Long,
    endTime: Long,
    cells: Set[(Int, Int)],
    firstEventBeginTime: Long,
    lastEventEndTime: Long,
    outMinSpeed: Double,
    intraMinSpeedSum: Double,
    numEvents: Int,
    countryIsoCode: String = CountryCode.SaudiArabiaIsoCode,
    score: Option[CompatibilityScore] = None,
    typeEstimate: ModelEntityEstimate = DwellEstimate) extends CellsGeometry {

  lazy val avgIntraMinSpeed: Double = if (numEvents == 1) 0 else intraMinSpeedSum / (numEvents - 1)

  def append(event: Event)(implicit cellCatalogue: Map[(Int, Int), Cell]): SpatioTemporalSlot = {
    require(startTime <= event.beginTime)
    SpatioTemporalSlot(
      user = user,
      startTime = startTime,
      endTime = if (event.endTime > endTime) event.endTime else endTime,
      cells = cells + ((event.lacTac, event.cellId)),
      firstEventBeginTime = firstEventBeginTime,
      lastEventEndTime = if (event.endTime > lastEventEndTime) event.endTime else lastEventEndTime,
      outMinSpeed = event.outSpeed.getOrElse(Journey.ZeroSpeed.get),
      intraMinSpeedSum = intraMinSpeedSum + outMinSpeed,
      numEvents = numEvents + 1,
      countryIsoCode = countryIsoCode)
  }

  def append(slot: SpatioTemporalSlot): SpatioTemporalSlot = {
    require(startTime <= slot.startTime)
    SpatioTemporalSlot(
      user = user,
      startTime = startTime,
      endTime = if (slot.endTime > endTime) slot.endTime else endTime,
      cells = cells ++ slot.cells,
      firstEventBeginTime = firstEventBeginTime,
      lastEventEndTime = if (slot.lastEventEndTime > lastEventEndTime) slot.lastEventEndTime else lastEventEndTime,
      outMinSpeed = slot.outMinSpeed,
      intraMinSpeedSum = intraMinSpeedSum + outMinSpeed + slot.intraMinSpeedSum,
      numEvents = numEvents + slot.numEvents,
      countryIsoCode = countryIsoCode)
  }

  def fields(implicit cellCatalogue: Map[(Int, Int), Cell]): Array[String] =
    user.fields ++
      Array(
        EdmCoreUtils.fmt.print(startTime),
        EdmCoreUtils.fmt.print(endTime),
        numEvents.toString,
        outMinSpeed.toString,
        avgIntraMinSpeed.toString,
        cells.mkString(EdmCoreUtils.IntraSequenceSeparator),
        EdmCoreUtils.fmt.print(firstEventBeginTime),
        EdmCoreUtils.fmt.print(lastEventEndTime),
        GeomUtils.wkt(geom),
        countryIsoCode,
        typeEstimate.id)
}

object SpatioTemporalSlot {

  def apply(event: Event)(implicit cellCatalogue: Map[(Int, Int), Cell]): SpatioTemporalSlot =
    SpatioTemporalSlot(
      user = event.user,
      startTime = event.beginTime,
      endTime = event.endTime,
      cells = Set((event.lacTac, event.cellId)),
      firstEventBeginTime = event.beginTime,
      lastEventEndTime = event.endTime,
      outMinSpeed = event.outSpeed.getOrElse(Journey.ZeroSpeed.get),
      intraMinSpeedSum = 0,
      numEvents = 1,
      countryIsoCode = Coordinates.utmSridIsoCode(cellCatalogue.head._2.planarCoords.srid))

  def header: Array[String] =
    User.header ++
      Array("startTime", "endTime", "numEvents", "outMinSpeed", "avgIntraMinSpeed", "cells", "firstEventBeginTime",
        "lastEventEndTime", "geomWkt", "countryIsoCode", "typeEstimate")
}
